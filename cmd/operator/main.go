package main

import (
	"flag"
	"fmt"

	"github.com/go-logr/zapr"
	"github.com/runningman84/zfs-snapshot-operator/pkg/config"
	"github.com/runningman84/zfs-snapshot-operator/pkg/operator"
	"go.uber.org/zap"
	"k8s.io/klog/v2"
)

// Version can be set at build time using -ldflags
// Example: go build -ldflags="-X main.Version=1.0.0"
var Version = "dev"

func main() {
	// Initialize klog first
	klog.InitFlags(nil)

	// Parse command line flags
	mode := flag.String("mode", "direct", "Operation mode: test, direct, or chroot")
	logLevel := flag.String("log-level", "info", "Log level: info or debug")
	logFormat := flag.String("log-format", "text", "Log format: text or json")
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
		klog.Fatalf("Invalid mode: %s. Must be one of: test, direct, chroot", *mode)
	}

	// Validate log level
	if *logLevel != "info" && *logLevel != "debug" {
		klog.Fatalf("Invalid log level: %s. Must be one of: info, debug", *logLevel)
	}

	// Validate and set log format
	if *logFormat != "text" && *logFormat != "json" {
		klog.Fatalf("Invalid log format: %s. Must be one of: text, json", *logFormat)
	}
	if *logFormat == "json" {
		// Configure zap for JSON logging
		var zapLog *zap.Logger
		var err error
		if *logLevel == "debug" {
			zapLog, err = zap.NewDevelopment()
		} else {
			zapLog, err = zap.NewProduction()
		}
		if err != nil {
			klog.Fatalf("Failed to initialize JSON logger: %v", err)
		}
		defer zapLog.Sync()

		// Set klog to use zap backend for JSON output
		klog.SetLogger(zapr.NewLogger(zapLog))
	}

	klog.Infof("Starting zfs-snapshot-operator version %s in %s mode with %s log level", Version, *mode, *logLevel)

	// Create configuration with specified mode
	cfg := config.NewConfig(*mode)
	cfg.LogLevel = *logLevel

	// Set klog verbosity based on log level
	if *logLevel == "debug" {
		flag.Set("v", "1")
	}

	// Override DryRun if specified via flag
	if *dryRun {
		cfg.DryRun = true
		klog.Infof("Dry-run mode enabled via command-line flag")
	}

	// Create and run operator
	op := operator.NewOperator(cfg)
	if err := op.Run(); err != nil {
		klog.Fatalf("Operator failed: %v", err)
	}

	klog.Flush()
}
