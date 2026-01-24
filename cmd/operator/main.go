package main

import (
	"log"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/operator"
)

func main() {
	// Create configuration (TestMode is disabled for production)
	cfg := config.NewConfig(false)

	// Create and run operator
	op := operator.NewOperator(cfg)
	if err := op.Run(); err != nil {
		log.Fatalf("Operator failed: %v", err)
	}
}
