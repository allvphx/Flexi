package storage

import (
	"FC/configs"
	"testing"
)

func TestTPCCStorageKit(t *testing.T) {
	configs.OuAddress = []string{"127.0.0.1:6001", "127.0.0.1:6002", "127.0.0.1:6003"}
	_ = TPCCStorageKit("127.0.0.1:5001")
}
