package channelgateway

import "encoding/json"

type sealedBinding struct {
	ChannelID string `json:"channel_id"`
}

func (s *Store) alias(domain, value string) string {
	if value == "" {
		return ""
	}
	return s.spool.Alias(domain, value)
}

func (s *Store) saveBinding(conversationID, channelID string) error {
	data, err := json.Marshal(sealedBinding{ChannelID: channelID})
	if err != nil {
		return ErrStorage
	}
	if err := s.spool.WriteImmutable("binding", s.alias("conversation", conversationID), data); err != nil {
		return ErrStorage
	}
	return nil
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

func (s *Store) saveInbound(eventAlias, body string) error {
	if err := s.spool.WriteImmutable("inbound", eventAlias, []byte(body)); err != nil {
		return ErrStorage
	}
	return nil
}

func (s *Store) inboundBody(eventAlias string) (string, error) {
	data, err := s.spool.Read("inbound", eventAlias)
	if err != nil {
		return "", ErrStorage
	}
	return string(data), nil
}

func (s *Store) saveOutbound(itemID, body string) error {
	if err := s.spool.WriteImmutable("outbound", s.alias("outbox", itemID), []byte(body)); err != nil {
		return ErrStorage
	}
	return nil
}

func (s *Store) outboundBody(itemID string) (string, error) {
	data, err := s.spool.Read("outbound", s.alias("outbox", itemID))
	if err != nil {
		return "", ErrStorage
	}
	return string(data), nil
}

func (s *Store) saveProviderMessage(itemID, providerID string) error {
	if err := s.spool.WriteImmutable("provider", s.alias("outbox", itemID), []byte(providerID)); err != nil {
		return ErrStorage
	}
	return nil
}

func (s *Store) providerMessage(itemID string) (string, error) {
	data, err := s.spool.Read("provider", s.alias("outbox", itemID))
	if err != nil {
		return "", ErrStorage
	}
	return string(data), nil
}

func (s *Store) saveThreadID(exact string) error {
	if exact == "" {
		return nil
	}
	if err := s.spool.WriteImmutable("thread", s.alias("thread", exact), []byte(exact)); err != nil {
		return ErrStorage
	}
	return nil
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
