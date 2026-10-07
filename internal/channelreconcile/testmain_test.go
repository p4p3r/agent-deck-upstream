package channelreconcile

import (
	"bytes"
	"encoding/base64"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("SLACK_DECK_SPOOL_KEY", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 32)))
	os.Exit(m.Run())
}
