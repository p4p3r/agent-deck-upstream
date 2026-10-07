package channelgateway

import (
	"encoding/base64"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("SLACK_DECK_SPOOL_KEY", base64.StdEncoding.EncodeToString(makeSyntheticKey()))
	os.Exit(m.Run())
}

func makeSyntheticKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = 0x42
	}
	return key
}
