package channelgateway

import "encoding/json"

type sealedBinding struct {
	ConversationID string `json:"conversation_id"`
	ChannelID      string `json:"channel_id"`
}

func (s *Store) alias(domain, value string) string {
	if value == "" {
		return ""
	}
	return s.spool.Alias(domain, value)
}

func (s *Store) seal(tx *writeTx, domain, alias string, plain []byte) error {
	if tx != nil {
		return tx.seal(domain, alias, plain)
	}
	if s.spool.WriteImmutable(domain, alias, plain) != nil {
		return ErrStorage
	}
	return nil
}

func (s *Store) saveBinding(tx *writeTx, conversationID, channelID string) error {
	alias := s.alias("conversation", conversationID)
	if old, err := s.spool.Read("binding", alias); err == nil {
		var binding sealedBinding
		if json.Unmarshal(old, &binding) == nil && binding.ConversationID == "" && binding.ChannelID == channelID {
			// Earlier v5 records contain only the channel. Keep their durable
			// ciphertext byte-for-byte on an idempotent reopen.
			return nil
		}
	}
	data, err := json.Marshal(sealedBinding{ConversationID: conversationID, ChannelID: channelID})
	if err != nil {
		return ErrStorage
	}
	return s.seal(tx, "binding", alias, data)
}

func (s *Store) channelID(conversationID string) (string, error) {
	data, err := s.spool.Read("binding", s.alias("conversation", conversationID))
	if err != nil {
		return "", ErrStorage
	}
	var binding sealedBinding
	if json.Unmarshal(data, &binding) != nil || binding.ChannelID == "" {
		return "", ErrStorage
	}
	return binding.ChannelID, nil
}

func (s *Store) saveInbound(tx *writeTx, eventAlias, body string) error {
	return s.seal(tx, "inbound", eventAlias, []byte(body))
}

func (s *Store) inboundBody(eventAlias string) (string, error) {
	data, err := s.spool.Read("inbound", eventAlias)
	if err != nil {
		return "", ErrStorage
	}
	return string(data), nil
}

func (s *Store) saveOutbound(tx *writeTx, itemID, body string) error {
	return s.seal(tx, "outbound", s.alias("outbox", itemID), []byte(body))
}

func (s *Store) outboundBody(itemID string) (string, error) {
	data, err := s.spool.Read("outbound", s.alias("outbox", itemID))
	if err != nil {
		return "", ErrStorage
	}
	return string(data), nil
}

func (s *Store) saveProviderMessage(tx *writeTx, itemID, providerID string) error {
	return s.seal(tx, "provider", s.alias("outbox", itemID), []byte(providerID))
}

func (s *Store) providerMessage(itemID string) (string, error) {
	data, err := s.spool.Read("provider", s.alias("outbox", itemID))
	if err != nil {
		return "", ErrStorage
	}
	return string(data), nil
}

func (s *Store) saveThreadID(tx *writeTx, exact string) error {
	if exact == "" {
		return nil
	}
	return s.seal(tx, "thread", s.alias("thread", exact), []byte(exact))
}

func (s *Store) exactThread(alias string) (string, error) {
	if alias == "" {
		return "", nil
	}
	data, err := s.spool.Read("thread", alias)
	if err != nil || s.alias("thread", string(data)) != alias {
		return "", ErrStorage
	}
	return string(data), nil
}
