//go:build !linux

package main

import "os"

func runningBinarySHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	return sha256File(path)
}
