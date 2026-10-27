# 0013. Local storage placement: HDD for lake and raw files, SSD for state

Status: Accepted (2026-10-28)

## Context
The development machine has:
- an SSD with 23 GB free
- an external HDD at `/media/akasdh/AKASH..` (Toshiba MQ01ABD100, 931 GB, 194 GB free), mounted as NTFS via
  ntfs-3g (`fuseblk`)

The 35-day local window needs about 90 GB of lake and raw files.

NTFS through FUSE does not provide POSIX ownership or permission bits, and its fsync is slow on a spinning disk.

## Decision
- **On the HDD,** under `${WIKIPULSE_DATA_ROOT}` (`/media/akasdh/AKASH../wikipulse` on this machine):
  - the DuckLake data path (bronze, silver and gold Parquet)
  - raw source files (pageview .gz, XML parts, History TSV .bz2)
  - evaluation label snapshots

  These are large and written sequentially, which an HDD handles at about 100 MB/s.
- **On the SSD, as Docker named volumes:**
  - Postgres data
  - Kafka log directories
  - Quix RocksDB state
  - the DuckDB `temp_directory`, capped at 4 GB

  Together these are under 6 GB.
- `WIKIPULSE_DATA_ROOT` is set in the gitignored `.env`. The committed default `./data` keeps CI and other machines
  working.
- Services that use the data root check at startup and in `/readyz` that the path is a mounted, writable
  directory containing a marker file. If the drive is not mounted, they fail instead of writing into an empty
  mountpoint on the SSD.

## Alternatives considered
- **Everything on the SSD.** 23 GB does not hold the 35-day window. Raw files could not be retained, which would
  weaken reproducibility of filtering.
- **Postgres or Kafka on the HDD.** Postgres refuses a data directory without 0700 ownership, which NTFS-FUSE
  cannot provide. Kafka and RocksDB fsync on every batch, which is slow on a spinning disk through FUSE.
- **Reformatting the HDD to ext4.** It already holds 107 GB of the user's data.

## Consequences
- Lake scans are bound by HDD throughput. The build measures dbt and backfill run times on this disk and reports
  them.
- If the drive is unplugged, writes stop and readiness fails. Kafka retains up to 3 days of stream, so the
  stream sink catches up after the drive returns.
- Paths on the HDD are case-insensitive under NTFS. Partition directory names use lowercase values only.
