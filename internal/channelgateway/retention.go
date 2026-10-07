package channelgateway

import (
	"context"
	"time"
)

// RetentionPolicy applies only to terminal work. Zero durations select the
// documented defaults; negative durations and unbounded batches are rejected.
type RetentionPolicy struct {
	DeliveredContent time.Duration
	UncertainContent time.Duration
	Metadata         time.Duration
}

func (p RetentionPolicy) normalized() (RetentionPolicy, bool) {
	if p.DeliveredContent < 0 || p.UncertainContent < 0 || p.Metadata < 0 {
		return p, false
	}
	if p.DeliveredContent == 0 {
		p.DeliveredContent = 24 * time.Hour
	}
	if p.UncertainContent == 0 {
		p.UncertainContent = 7 * 24 * time.Hour
	}
	if p.Metadata == 0 {
		p.Metadata = 90 * 24 * time.Hour
	}
	return p, p.Metadata >= p.UncertainContent && p.UncertainContent >= p.DeliveredContent
}

type RetentionResult struct {
	ContentDeleted  int
	MetadataDeleted int
	OrphansDeleted  int
}

type expiredRecord struct {
	domain string
	alias  string
	id     string
}

// Prune is an explicit bounded operation. SQL first records expiry, then the
// sealed files are unlinked, then SQL records completed deletion. A crash at
// any point is repaired by the next call. Uncertain rows retain their ledger
// ownership and state even after their ciphertext has expired.
func (s *Store) Prune(ctx context.Context, policy RetentionPolicy, maxRows int) (RetentionResult, error) {
	var result RetentionResult
	policy, ok := policy.normalized()
	if s == nil || ctx == nil || !ok || maxRows < 1 || maxRows > 1024 {
		return result, ErrInvalid
	}
	now := s.now().UTC().Unix()
	deliveredCutoff := now - int64(policy.DeliveredContent/time.Second)
	uncertainCutoff := now - int64(policy.UncertainContent/time.Second)
	metadataCutoff := now - int64(policy.Metadata/time.Second)
	var files []expiredRecord
	err := s.write(ctx, func(tx *writeTx) error {
		if _, err := tx.exec(`UPDATE outbox SET content_expired_at=? WHERE id IN (
			SELECT o.id FROM outbox o JOIN turns t ON t.id=o.turn_id
			WHERE t.status='completed' AND o.content_expired_at=0 AND
			 ((o.state='delivered' AND o.delivered_at>0 AND o.delivered_at<=?) OR
			  (o.state='uncertain' AND o.uncertain_at>0 AND o.uncertain_at<=?))
			ORDER BY o.ordinal LIMIT ?)`, now, deliveredCutoff, uncertainCutoff, maxRows); err != nil {
			return err
		}
		if _, err := tx.exec(`UPDATE inbound_events SET content_expired_at=? WHERE ordinal IN (
			SELECT e.ordinal FROM inbound_events e JOIN turns t ON t.event_ordinal=e.ordinal
			WHERE e.content_ref!='' AND e.content_expired_at=0 AND t.status='completed'
			AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.turn_id=t.id AND o.state IN ('pending','sending'))
			AND ((NOT EXISTS (SELECT 1 FROM outbox o WHERE o.turn_id=t.id) AND t.completed_at>0 AND t.completed_at<=?)
			 OR (EXISTS (SELECT 1 FROM outbox o WHERE o.turn_id=t.id) AND
			     NOT EXISTS (SELECT 1 FROM outbox o WHERE o.turn_id=t.id AND o.content_expired_at=0)))
			ORDER BY e.ordinal LIMIT ?)`, now, deliveredCutoff, maxRows); err != nil {
			return err
		}
		rows, err := tx.conn.QueryContext(ctx, `SELECT id,kind FROM outbox WHERE content_expired_at>0 AND content_deleted_at=0 ORDER BY ordinal LIMIT ?`, maxRows)
		if err != nil {
			return ErrStorage
		}
		for rows.Next() {
			var id, kind string
			if rows.Scan(&id, &kind) != nil {
				_ = rows.Close()
				return ErrStorage
			}
			files = append(files, expiredRecord{domain: "outbox", id: id, alias: kind})
		}
		if rows.Err() != nil {
			_ = rows.Close()
			return ErrStorage
		}
		_ = rows.Close()
		if len(files) >= maxRows {
			return nil
		}
		rows, err = tx.conn.QueryContext(ctx, `SELECT event_id FROM inbound_events WHERE content_expired_at>0 AND content_deleted_at=0 ORDER BY ordinal LIMIT ?`, maxRows-len(files))
		if err != nil {
			return ErrStorage
		}
		for rows.Next() {
			var alias string
			if rows.Scan(&alias) != nil {
				_ = rows.Close()
				return ErrStorage
			}
			files = append(files, expiredRecord{domain: "inbound", alias: alias})
		}
		if rows.Err() != nil {
			_ = rows.Close()
			return ErrStorage
		}
		_ = rows.Close()
		return nil
	})
	if err != nil {
		return result, err
	}
	for _, file := range files {
		if file.domain == "outbox" {
			if file.alias == "reply" && s.spool.Remove("outbound", s.alias("outbox", file.id)) != nil {
				return result, ErrStorage
			}
			if s.spool.Remove("provider", s.alias("outbox", file.id)) != nil {
				return result, ErrStorage
			}
		} else if s.spool.Remove("inbound", file.alias) != nil {
			return result, ErrStorage
		}
		err := s.write(ctx, func(tx *writeTx) error {
			if file.domain == "outbox" {
				_, err := tx.exec(`UPDATE outbox SET content_deleted_at=? WHERE id=? AND content_expired_at>0`, now, file.id)
				return err
			}
			_, err := tx.exec(`UPDATE inbound_events SET content_deleted_at=? WHERE event_id=? AND content_expired_at>0`, now, file.alias)
			return err
		})
		if err != nil {
			return result, err
		}
		result.ContentDeleted++
	}
	metadataBudget := maxRows - result.ContentDeleted
	if metadataBudget == 0 {
		return result, nil
	}
	err = s.write(ctx, func(tx *writeTx) error {
		rows, err := tx.conn.QueryContext(ctx, `SELECT t.id,e.ordinal FROM turns t JOIN inbound_events e ON e.ordinal=t.event_ordinal
			WHERE t.status='completed' AND t.completed_at>0 AND t.completed_at<=? AND
			(e.content_ref='' OR e.content_deleted_at>0) AND
			NOT EXISTS (SELECT 1 FROM outbox o WHERE o.turn_id=t.id AND (o.state!='delivered' OR o.content_deleted_at=0))
			ORDER BY e.ordinal LIMIT ?`, metadataCutoff, metadataBudget)
		if err != nil {
			return ErrStorage
		}
		type oldTurn struct {
			id      string
			ordinal int64
		}
		var turns []oldTurn
		for rows.Next() {
			var turn oldTurn
			if rows.Scan(&turn.id, &turn.ordinal) != nil {
				_ = rows.Close()
				return ErrStorage
			}
			turns = append(turns, turn)
		}
		if rows.Err() != nil {
			_ = rows.Close()
			return ErrStorage
		}
		_ = rows.Close()
		for _, turn := range turns {
			if _, err := tx.exec(`DELETE FROM outbox WHERE turn_id=? AND state='delivered' AND content_deleted_at>0`, turn.id); err != nil {
				return err
			}
			if _, err := tx.exec(`DELETE FROM turns WHERE id=?`, turn.id); err != nil {
				return err
			}
			if _, err := tx.exec(`DELETE FROM inbound_events WHERE ordinal=?`, turn.ordinal); err != nil {
				return err
			}
			result.MetadataDeleted++
		}
		if result.MetadataDeleted < metadataBudget {
			r, err := tx.exec(`DELETE FROM inbound_events WHERE ordinal IN (
				SELECT ordinal FROM inbound_events WHERE disposition!='accepted' AND seen_at>0 AND seen_at<=?
				ORDER BY ordinal LIMIT ?)`, metadataCutoff, metadataBudget-result.MetadataDeleted)
			if err != nil {
				return err
			}
			deleted, err := r.RowsAffected()
			if err != nil {
				return ErrStorage
			}
			result.MetadataDeleted += int(deleted)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	remaining := maxRows - result.ContentDeleted - result.MetadataDeleted
	if remaining > 0 {
		removed, err := s.spool.CleanupOrphans(s.now().Add(-policy.DeliveredContent), remaining)
		if err != nil {
			return result, ErrStorage
		}
		result.OrphansDeleted = removed
	}
	return result, nil
}
