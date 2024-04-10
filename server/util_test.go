package server

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGenerateSessionID(t *testing.T) {
	testAddr := "127.0.0.1:1234"
	sessionID := generateSessionID(testAddr)
	decodedAddr := extractAddrFromSessionID(sessionID)
	require.Equal(t, testAddr, decodedAddr)

	// old format
	require.Equal(t, testAddr, extractAddrFromSessionID(testAddr))
}
