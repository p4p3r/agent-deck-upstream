package channelgateway

import (
	"context"
	"time"
)

// HealthProjection contains bounded routing metadata only. It never reads a
// sealed record or returns an external identifier, message body, or row ID.
type HealthProjection struct {
	BacklogState     string `json:"backlog_state"`
	OldestAgeSeconds *int64 `json:"oldest_age_seconds,omitempty"`
	EgressState      string `json:"egress_state"`
	ConductorState   string `json:"conductor_state"`
	TurnAgeSeconds   *int64 `json:"turn_age_seconds,omitempty"`
}

func boundedAge(now time.Time, then int64) int64 {
	age := now.UTC().Unix() - then
	if age < 0 {
		return 0
	}
	if age > 315_360_000 {
		return 315_360_000
	}
	return age
}

// HealthProjection reads all status inputs in one bounded, body-free query.
// A sending row is unknown rather than clear until its exact outcome commits.
func (s *Store) HealthProjection(ctx context.Context, conversationID string, now time.Time) (HealthProjection, error) {
	var result HealthProjection
	if s == nil || s.db == nil || ctx == nil || conversationID == "" || now.IsZero() {
		return result, ErrInvalid
	}
	var conversations, backlog, uncertain, sending, working int
	var oldest, turnStarted int64
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM conversations WHERE id=?),
		(SELECT count(*) FROM inbound_events e LEFT JOIN turns t ON t.id=e.turn_id
		 WHERE e.conversation_id=? AND e.disposition='accepted' AND
		  (e.turn_id IS NULL OR t.status='active' OR EXISTS
		   (SELECT 1 FROM outbox o WHERE o.turn_id=t.id AND o.state IN ('pending','sending','uncertain')))),
		COALESCE((SELECT min(e.seen_at) FROM inbound_events e LEFT JOIN turns t ON t.id=e.turn_id
		 WHERE e.conversation_id=? AND e.disposition='accepted' AND
		  (e.turn_id IS NULL OR t.status='active' OR EXISTS
		   (SELECT 1 FROM outbox o WHERE o.turn_id=t.id AND o.state IN ('pending','sending','uncertain')))),0),
		(SELECT count(*) FROM outbox WHERE conversation_id=? AND state='uncertain'),
		(SELECT count(*) FROM outbox WHERE conversation_id=? AND state='sending'),
		(SELECT count(*) FROM turns WHERE conversation_id=? AND status='active'),
		COALESCE((SELECT min(e.seen_at) FROM turns t JOIN inbound_events e ON e.ordinal=t.event_ordinal
		 WHERE t.conversation_id=? AND t.status='active'),0)`,
		conversationID, conversationID, conversationID, conversationID, conversationID, conversationID, conversationID).
		Scan(&conversations, &backlog, &oldest, &uncertain, &sending, &working, &turnStarted)
	if err != nil {
		return HealthProjection{}, ErrStorage
	}
	if conversations != 1 || working > 1 {
		return HealthProjection{}, ErrNotFound
	}
	result.BacklogState = "empty"
	if backlog > 0 {
		result.BacklogState = "pending"
		age := boundedAge(now, oldest)
		result.OldestAgeSeconds = &age
	}
	result.EgressState = "clear"
	if uncertain > 0 {
		result.EgressState = "uncertain"
	} else if sending > 0 {
		result.EgressState = "unknown"
	}
	result.ConductorState = "idle"
	if working == 1 {
		result.ConductorState = "working"
		age := boundedAge(now, turnStarted)
		result.TurnAgeSeconds = &age
	}
	return result, nil
}
