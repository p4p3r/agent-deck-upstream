//go:build !linux

package slackcontrol

import (
	"errors"
	"net"
	"os"
)

func peerUID(*net.UnixConn) (int, error)              { return -1, errors.New("peer credentials unavailable") }
func ownedBy(os.FileInfo, int) bool                   { return false }
func singleLink(os.FileInfo) bool                     { return false }
func fileIdentity(os.FileInfo) (uint64, uint64, bool) { return 0, 0, false }
