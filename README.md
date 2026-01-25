# ZFS Snapshot Operator

A Kubernetes operator for automated ZFS snapshot management with configurable retention policies and pool health monitoring.

> **Note**: This operator is designed for Talos-based NAS systems and requires deployment as a separate Helm release for each node with ZFS pools.

## Features

- **Automated Snapshot Management**: Creates and manages snapshots at configurable frequencies (hourly, daily, weekly, monthly, yearly)
- **Flexible Retention Policies**: Configurable maximum snapshot counts per frequency via environment variables
- **Pool Filtering**: Whitelist specific ZFS pools to manage
- **Filesystem Filtering**: Whitelist specific filesystems to manage
- **Health Monitoring**:
  - Checks pool health status and warns about degraded pools
  - Warns when pool scrubs are older than 90 days
  - Logs pool errors (read, write, checksum errors)
  - Logs filesystem usage statistics
- **Safety Features**:
  - **Create-first approach**: New snapshots are created before any deletions to prevent loss of backup protection
  - **Fail-safe behavior**: If snapshot creation fails (disk errors, filesystem full, etc.), no deletions occur
  - **Dry-run mode**: Preview snapshot operations (create/delete) without actually executing them
  - **Deletion limits**: Maximum number of snapshots to delete per run
  - **Concurrent run protection**: Lock file prevents multiple instances running simultaneously
  - **Error exit codes**: Exits with code 1 if any pool is unhealthy or commands fail
- **Kubernetes Native**: Runs as a CronJob with configurable scheduling
- **Test Mode**: Built-in test mode for development and validation
- **Debug Logging**: Detailed command execution logs in debug mode

## Architecture

The operator is designed to run as a Kubernetes CronJob **every hour** (recommended schedule: `0 * * * *`).

Each run performs the following operations:
1. Lists all ZFS pools and snapshots on the host
2. Filters pools based on whitelist configuration (if specified)
3. Checks pool health status and scrub age
4. **Creates new snapshots first** (if needed) - this happens before any deletions
5. Cleans up old snapshots according to retention policies

**Important:** The operator is stateless and resilient to scheduling gaps. If it hasn't run for some time (e.g., due to cluster downtime or maintenance), it will still work correctly when it resumes. The time-window retention logic ensures proper snapshot coverage based on actual snapshot ages, not execution frequency.

**Safety-First Design:** New snapshots are always created before any old snapshots are deleted. If snapshot creation fails (e.g., due to disk issues or filesystem errors), the operator aborts the run without deleting any existing snapshots, ensuring your backup protection is never reduced.

## Prerequisites

- Kubernetes cluster with host access (hostPID required)
- ZFS installed on the host nodes
- Host root filesystem mounted in the container

## Installation

### Download Pre-built Binaries

Download the latest release for your platform:

```bash
# AMD64
wget https://github.com/runningman84/zfs-snapshot-operator/releases/latest/download/zfs-snapshot-operator-linux-amd64.tar.gz
tar -xzf zfs-snapshot-operator-linux-amd64.tar.gz
./zfs-snapshot-operator-linux-amd64 -version

# ARM64
wget https://github.com/runningman84/zfs-snapshot-operator/releases/latest/download/zfs-snapshot-operator-linux-arm64.tar.gz
tar -xzf zfs-snapshot-operator-linux-arm64.tar.gz
./zfs-snapshot-operator-linux-arm64 -version

# Verify checksum
wget https://github.com/runningman84/zfs-snapshot-operator/releases/latest/download/zfs-snapshot-operator-linux-amd64.tar.gz.sha256
sha256sum -c zfs-snapshot-operator-linux-amd64.tar.gz.sha256
```

### Using Helm

```bash
# Install with default values
helm install zfs-snapshot-operator ./helm

# Install with custom values
helm install zfs-snapshot-operator ./helm -f custom-values.yaml
```

### Manual Installation

```bash
# Build the Docker image
docker build -t zfs-snapshot-operator:latest .

# Apply Kubernetes manifests
kubectl apply -f helm/templates/
```

### Important Deployment Notes

**Per-Node Installation Required:**

Since this operator accesses ZFS pools directly on the host via `hostPID` and `chroot`, you must install a separate Helm release for each node that has ZFS pools. Each release should be configured to run only on its target node.

```bash
# Example: Install for node 'nas-node-1'
helm install zfs-snapshot-operator-nas-1 oci://ghcr.io/runningman84/charts/zfs-snapshot-operator \
  --version 1.2.2 \
  --namespace zfs-snapshot-operator \
  --create-namespace \
  --set nodeSelector.kubernetes\.io/hostname=nas-node-1 \
  --set pools.whitelist="tank,backup"

# Example: Install for node 'nas-node-2'
helm install zfs-snapshot-operator-nas-2 oci://ghcr.io/runningman84/charts/zfs-snapshot-operator \
  --version 1.2.2 \
  --namespace zfs-snapshot-operator \
  --create-namespace \
  --set nodeSelector.kubernetes\.io/hostname=nas-node-2 \
  --set pools.whitelist="storage"
```

**Talos Linux Considerations:**

This operator is specifically designed for Talos-based NAS systems where:
- ZFS is installed on Talos nodes
- The operator uses `chroot /host` to access the host's ZFS utilities
- Host filesystem is mounted at `/host` in the container
- Requires privileged access with `SYS_ADMIN` and `SYS_CHROOT` capabilities

## Configuration

### Environment Variables

The operator is configured through environment variables, which can be set in the Helm values file:

| Variable | Description | Default |
|----------|-------------|---------|
| `LOG_LEVEL` | Log level: `info` or `debug` (debug prints all executed commands) | `info` |
| `DRY_RUN` | If `true`, log what would be created/deleted but don't actually modify snapshots | `false` |
| `MAX_DELETIONS_PER_RUN` | Maximum number of snapshots to delete in a single run (safety limit) | `100` |
| `LOCK_FILE_PATH` | Path to lock file for preventing concurrent runs | `/tmp/zfs-snapshot-operator.lock` |
| `MAX_HOURLY_SNAPSHOTS` | Maximum number of hourly snapshots to retain | `24` |
| `MAX_DAILY_SNAPSHOTS` | Maximum number of daily snapshots to retain | `7` |
| `MAX_WEEKLY_SNAPSHOTS` | Maximum number of weekly snapshots to retain | `4` |
| `MAX_MONTHLY_SNAPSHOTS` | Maximum number of monthly snapshots to retain | `12` |
| `MAX_YEARLY_SNAPSHOTS` | Maximum number of yearly snapshots to retain | `3` |
| `POOL_WHITELIST` | Comma-separated list of pools to manage (empty = all pools) | `""` |
| `FILESYSTEM_WHITELIST` | Comma-separated list of filesystems to manage (empty = all filesystems) | `""` |
| `SNAPSHOT_PREFIX` | Prefix for automatic snapshot names | `autosnap` |
| `SCRUB_AGE_THRESHOLD_DAYS` | Number of days before warning about old scrubs | `90` |
| `CHROOT_HOST_PATH` | Host root path for chroot mode | `/host` |
| `CHROOT_BIN_PATH` | Path to ZFS binaries in chroot mode | `/usr/local/sbin` |

### Helm Values

Edit [helm/values.yaml](helm/values.yaml) to customize the deployment:

```yaml
# CronJob schedule (default: every hour)
cronjob:
  schedule: "0 * * * *"

# Snapshot retention
snapshots:
  maxHourly: 24
  maxDaily: 7
  maxWeekly: 4
  maxMonthly: 12
  maxYearly: 3

# Pool filtering
pools:
  whitelist: ""  # Example: "tank,backup"

# Pool health monitoring
monitoring:
  scrubAgeThresholdDays: 90  # Warn if scrub older than this many days
```

### Example Configurations

**Only manage specific pools:**
```yaml
pools:
  whitelist: "tank,backup"
```

**Custom snapshot prefix:**
```yaml
snapshotPrefix: "zfs-auto"  # Snapshots will be named: zfs-auto_2026-01-25_14:00:00_hourly
```

**Aggressive retention (more snapshots):**
```yaml
snapshots:
  maxHourly: 48    # 2 days of hourly
  maxDaily: 14     # 2 weeks of daily
  maxWeekly: 8     # 2 months of weekly
  maxMonthly: 24   # 2 years of monthly
  maxYearly: 10    # 10 years of yearly
```

**Conservative retention (fewer snapshots):**
```yaml
snapshots:
  maxHourly: 12    # 12 hours
  maxDaily: 3      # 3 days
  maxWeekly: 2     # 2 weeks
  maxMonthly: 6    # 6 months
  maxYearly: 1     # 1 year
```

**Custom scrub monitoring:**
```yaml
monitoring:
  scrubAgeThresholdDays: 180  # Warn if scrub older than 6 months

# Or more aggressive monitoring:
monitoring:
  scrubAgeThresholdDays: 30   # Warn if scrub older than 1 month
```

**Safety features:**
```yaml
operator:
  # Enable dry-run mode to preview what would be deleted
  dryRun: true

  # Limit maximum deletions per run (safety against bugs)
  maxDeletionsPerRun: 50

  # Custom lock file path
  lockFilePath: /var/run/zfs-operator.lock
```

**Testing configuration before deployment:**
```yaml
# First deploy with dry-run enabled
operator:
  dryRun: true
  logLevel: debug

# Review logs, then disable dry-run for production
operator:
  dryRun: false
  logLevel: info
```

## Snapshot Naming Convention

Snapshots are created with the following naming pattern:
```
<prefix>_YYYY-MM-DD_HH:MM:SS_<frequency>
```

The default prefix is `autosnap` but can be customized via the `SNAPSHOT_PREFIX` environment variable.

Examples (with default prefix):
- `autosnap_2026-01-25_14:00:00_hourly`
- `autosnap_2026-01-25_00:00:00_daily`
- `autosnap_2026-01-20_00:00:00_weekly` (Mondays)
- `autosnap_2026-01-01_00:00:00_monthly` (1st of month)
- `autosnap_2026-01-01_00:00:00_yearly` (Jan 1st)

## Health Monitoring

The operator monitors ZFS pool health and provides warnings for:

### Scrub Age Monitoring

Warns when a pool's last scrub exceeds the configured threshold (default: 90 days). This threshold is configurable via the `SCRUB_AGE_THRESHOLD_DAYS` environment variable.

**Example warning:**
```
WARNING: Pool tank last scrub was 120 days ago (last scrub: 2025-10-26 02:00:00) - consider running 'zpool scrub tank'
```

**Configure threshold:**
```yaml
monitoring:
  scrubAgeThresholdDays: 30  # Warn after 30 days instead of default 90
```

### Pool States

The operator logs pool states and errors:
- `ONLINE`: Pool is healthy
- `DEGRADED`: Pool has issues but is accessible
- Errors: Logged error counts from pool status

## Development

### Command Line Options

```bash
# Show version
./operator -version

# Run with default mode (direct)
./operator

# Specify operation mode
./operator -mode <mode>

# Enable debug logging
./operator -log-level debug

# Enable dry-run mode (preview deletions without executing)
./operator -dry-run

# Combine options
./operator -mode chroot -log-level debug -dry-run
```

### Operation Modes

The operator supports three operation modes via the `-mode` flag:

**Test Mode** (`-mode test`):
- Uses test data files from `test/` directory
- No actual ZFS commands are executed
- Perfect for development and CI/CD testing
- Default in test environments

```bash
./operator -mode test
```

**Direct Mode** (`-mode direct`):
- Direct access to ZFS commands (default)
- Uses `zfs` and `zpool` from system $PATH
- No chroot wrapper
- Ideal for local development or when operator runs on the ZFS host directly

```bash
./operator -mode direct  # or just ./operator
```

**Chroot Mode** (`-mode chroot`):
- Production mode for containerized deployments
- Uses chroot to access host ZFS from container
- Commands use `chroot /host /usr/local/sbin/zfs` (configurable via `CHROOT_HOST_PATH` and `CHROOT_BIN_PATH`)
- Required when running in Kubernetes with hostPID

```bash
./operator -mode chroot

# Custom host mount path
CHROOT_HOST_PATH=/custom/host ./operator -mode chroot

# Custom ZFS binary path
CHROOT_BIN_PATH=/usr/sbin ./operator -mode chroot
```

### Building

```bash
# Build the binary
go build -o operator ./cmd/operator

# Run tests
go test ./pkg/...

# Run tests with coverage
go test ./pkg/... -cover
```

### Test Coverage

Current test coverage:
- `pkg/config`: 98.8%
- `pkg/parser`: 80.3%
- `pkg/zfs`: 49.6%
- `pkg/operator`: 10.7%

### Running in Test Mode

For development and testing, run the operator in test mode:

```bash
./operator -mode test
```

In test mode, the operator uses test data files from the `test/` directory instead of executing actual ZFS commands.

### Project Structure

```
.
├── cmd/
│   └── operator/
│       └── main.go           # Entry point
├── pkg/
│   ├── config/               # Configuration management
│   ├── models/               # Data models
│   ├── operator/             # Core operator logic
│   ├── parser/               # ZFS JSON parsing
│   └── zfs/                  # ZFS command execution
├── helm/                     # Helm chart
│   ├── templates/
│   └── values.yaml
├── test/                     # Test data files
│   ├── zfs_list_pools.json
│   ├── zfs_list_pools_empty.json
│   ├── zfs_list_snapshots.json
│   ├── zfs_list_snapshots_empty.json
│   ├── zpool_status.json
│   └── zpool_status_failed.json
└── Dockerfile
```

## How It Works

### Snapshot Creation Logic

1. **Hourly Snapshots**: Created every hour (when CronJob runs)
2. **Daily Snapshots**: Created once per day (at first run after midnight)
3. **Weekly Snapshots**: Created on Mondays (at first run on Monday)
4. **Monthly Snapshots**: Created on the 1st of each month
5. **Yearly Snapshots**: Created on January 1st

### Retention Logic

The operator uses **time-window retention with deduplication**:
- Divides time into periods based on frequency (hours, days, weeks, months, years)
- Keeps the **newest snapshot** from each period within the retention window
- Automatically deletes snapshots outside the retention window or duplicates within a period
- Preserves manual snapshots (not matching the configured prefix pattern)

**Safety-First Execution Order:**
1. **Create** new snapshots first (if needed)
2. If creation succeeds, **delete** old snapshots
3. If creation fails, abort without deleting anything

This ensures your backup protection never decreases. If ZFS has issues (disk errors, filesystem full, etc.), the operator fails early and preserves existing snapshots.

**Scheduling:** While the operator should run hourly for optimal coverage, it will function correctly even if it hasn't run for days or weeks. The retention logic is based on snapshot ages, not run frequency.

**Examples:**
- `maxYearly: 3` → Keeps the newest yearly snapshot from each of the last 3 years, deletes older
- `maxMonthly: 12` → Keeps the newest monthly snapshot from each of the last 12 months
- `maxDaily: 7` → Keeps the newest daily snapshot from each of the last 7 days

**Deduplication:** If multiple yearly snapshots exist in the same year (e.g., from manual creation or bugs), only the newest one is kept. This ensures you have temporal coverage rather than just the N most recent snapshots.

### Age Calculation

Snapshots are categorized by age:
- **Hourly**: Between 1 hour and 1 day old
- **Daily**: Between 1 day and 1 week old
- **Weekly**: Between 1 week and 1 month old
- **Monthly**: Between 1 month and 1 year old
- **Yearly**: Older than 1 year

## Security Considerations

The operator requires elevated privileges to manage ZFS:

```yaml
securityContext:
  allowPrivilegeEscalation: true
  capabilities:
    add:
      - SYS_ADMIN
      - SYS_CHROOT
  privileged: true
```

Additionally:
- Runs with `hostPID: true` to access host ZFS processes
- Mounts host root filesystem at `/host`
- Should only be deployed in trusted environments

## Troubleshooting

### Snapshots Not Being Created

1. Check CronJob execution:
   ```bash
   kubectl get cronjobs
   kubectl get jobs
   ```

2. Check operator logs:
   ```bash
   kubectl logs -l app.kubernetes.io/name=zfs-snapshot-operator
   ```

3. Verify ZFS is accessible:
   ```bash
   kubectl exec -it <pod-name> -- zpool list
   ```

### Pool Whitelist Not Working

Ensure the pool names in `pools.whitelist` exactly match the ZFS pool names:
```bash
# List pools
zpool list -H -o name

# Update Helm values
pools:
  whitelist: "exact-pool-name,another-pool"
```

### Scrub Warnings

If you see scrub warnings, run a scrub on the affected pool:
```bash
zpool scrub <pool-name>
```

To check scrub status:
```bash
zpool status <pool-name>
```

## Contributing

Contributions are welcome! Please ensure:
- All tests pass: `go test ./pkg/...`
- Code is formatted: `go fmt ./...`
- New features include tests
- Documentation is updated

## License

MIT License

## Credits

Developed by [runningman84](https://github.com/runningman84)

### Acknowledgments

- Snapshot naming convention inspired by [zfs-auto-snapshot](https://github.com/zfsonlinux/zfs-auto-snapshot)

## Support

For issues and questions:
- GitHub Issues: [Create an issue](https://github.com/runningman84/zfs-snapshot-operator/issues)
- Discussions: [GitHub Discussions](https://github.com/runningman84/zfs-snapshot-operator/discussions)
