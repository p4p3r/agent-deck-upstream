// Package slackcontrol serves the body-free Slack v2 health protocol over a
// private Unix socket.
package slackcontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
)

const MaxFrameBytes = 16 << 10

var (
	noncePattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	hashPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	aliasPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)
)

type Identity struct {
	BinarySHA256    string `json:"binary_sha256"`
	ConfigSHA256    string `json:"config_sha256"`
	RowBindingAlias string `json:"row_binding_alias"`
}

type RunnerStatus struct {
	State    string `json:"state"`
	ExitCode *int   `json:"exit_code"`
	Terminal bool   `json:"terminal"`
	Degraded bool   `json:"degraded"`
}

type PumpStatus struct {
	State                  string `json:"state"`
	LastProgressAgeSeconds *int64 `json:"last_progress_age_seconds"`
}

type BacklogStatus struct {
	State            string `json:"state"`
	OldestAgeSeconds *int64 `json:"oldest_age_seconds"`
}

type EgressStatus struct {
	State string `json:"state"`
}

type ConductorStatus struct {
	State          string `json:"state"`
	TurnAgeSeconds *int64 `json:"turn_age_seconds"`
}

type Status struct {
	Identity  Identity        `json:"identity"`
	Runner    RunnerStatus    `json:"runner"`
	Pump      PumpStatus      `json:"pump"`
	Backlog   BacklogStatus   `json:"backlog"`
	Egress    EgressStatus    `json:"egress"`
	Conductor ConductorStatus `json:"conductor"`
}

type requestFrame struct {
	Version int    `json:"version"`
	Type    string `json:"type"`
	Nonce   string `json:"nonce"`
}

type responseFrame struct {
	Version int    `json:"version"`
	Type    string `json:"type"`
	Nonce   string `json:"nonce"`
	Status
}

func parseRequest(data []byte, kind string) (requestFrame, error) {
	var frame requestFrame
	if !json.Valid(data) || rejectDuplicateJSONKeys(data) != nil {
		return frame, errors.New("invalid frame")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frame); err != nil {
		return requestFrame{}, errors.New("invalid frame")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return requestFrame{}, errors.New("invalid frame")
	}
	if frame.Version != 1 || frame.Type != kind || !noncePattern.MatchString(frame.Nonce) {
		return requestFrame{}, errors.New("invalid frame")
	}
	return frame, nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok || seen[key] {
				return errors.New("duplicate JSON key")
			}
			seen[key] = true
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("invalid JSON object")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("invalid JSON array")
		}
	default:
		return errors.New("invalid JSON delimiter")
	}
	return nil
}

func validAge(value *int64, required bool) bool {
	if value == nil {
		return !required
	}
	return required && *value >= 0 && *value <= 315_360_000
}

func (s Status) valid() bool {
	if !hashPattern.MatchString(s.Identity.BinarySHA256) || !hashPattern.MatchString(s.Identity.ConfigSHA256) ||
		!aliasPattern.MatchString(s.Identity.RowBindingAlias) {
		return false
	}
	switch s.Runner.State {
	case "running":
		if s.Runner.ExitCode != nil {
			return false
		}
	case "exited":
		if s.Runner.ExitCode == nil || *s.Runner.ExitCode < 0 || *s.Runner.ExitCode > 255 {
			return false
		}
	default:
		return false
	}
	switch s.Pump.State {
	case "fresh", "stale", "stalled":
		if !validAge(s.Pump.LastProgressAgeSeconds, true) {
			return false
		}
	case "idle", "unknown":
		if !validAge(s.Pump.LastProgressAgeSeconds, false) {
			return false
		}
	default:
		return false
	}
	switch s.Backlog.State {
	case "pending":
		if !validAge(s.Backlog.OldestAgeSeconds, true) {
			return false
		}
	case "empty", "unknown":
		if !validAge(s.Backlog.OldestAgeSeconds, false) {
			return false
		}
	default:
		return false
	}
	if s.Egress.State != "clear" && s.Egress.State != "uncertain" && s.Egress.State != "unknown" {
		return false
	}
	switch s.Conductor.State {
	case "working":
		if !validAge(s.Conductor.TurnAgeSeconds, true) {
			return false
		}
	case "idle", "unknown":
		if !validAge(s.Conductor.TurnAgeSeconds, false) {
			return false
		}
	default:
		return false
	}
	return !(s.Runner.State == "running" && s.Backlog.State == "pending" && s.Pump.State == "idle")
}
