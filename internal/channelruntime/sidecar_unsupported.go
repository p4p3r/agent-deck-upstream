//go:build !linux && !darwin

package channelruntime

import "os"

func ownedSingleLink(os.FileInfo) bool { return false }
