package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/operator"
)

// Version can be set at build time using -ldflags
// Example: go build -ldflags="-X main.Version=1.0.0"
var Version = "dev"

func main() {
	// Parse command line flags
	mode := flag.String("mode", "direct", "Operation mode: test, direct, or chroot")
	logLevel := flag.String("log-level", "info", "Log level: info or debug")
	dryRun := flag.Bool("dry-run", false, "Enable dry-run mode (no actual snapshot creation or deletion)")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	// Show version if requested
	if *showVersion {
		fmt.Printf("zfs-snapshot-operator version %s\n", Version)
		return
	}

	// Validate mode
	if *mode != "test" && *mode != "direct" && *mode != "chroot" {
		log.Fatalf("Invalid mode: %s. Must be one of: test, direct, chroot", *mode)
	}

	// Validate log level
	if *logLevel != "info" && *logLevel != "debug" {
		log.Fatalf("Invalid log level: %s. Must be one of: info, debug", *logLevel)
	}

	log.Printf("Starting zfs-snapshot-operator version %s in %s mode with %s log level", Version, *mode, *logLevel)

	// Create configuration with specified mode
	cfg := config.NewConfig(*mode)
	cfg.LogLevel = *logLevel

	// Override DryRun if specified via flag
	if *dryRun {
		cfg.DryRun = true
		log.Printf("Dry-run mode enabled via command-line flag")
	}

	// Create and run operator
	op := operator.NewOperator(cfg)
	if err := op.Run(); err != nil {
		log.Fatalf("Operator failed: %v", err)
	}
}
