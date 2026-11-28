"""Copy the versionable evidence of finished runs from runs/ (gitignored) to experiments/results/.

    python -m experiments.archive runs/f1-e2-base [runs/...] [--dest experiments/results/f1]

Kept as plain files: run.json, report.md, fault_windows.json, faults.jsonl, status/metrics/health
snapshots and client/timeline.csv. Compressed: client/latencies.csv (.gz), and the server logs and
SDK histories (one .tar.gz each). The dataset is left out: the runner regenerates it from the seed
recorded in run.json.
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLAIN = (
    "run.json",
    "report.md",
    "fault_windows.json",
    "faults.jsonl",
    "health_before.json",
    "status_before.json",
    "status_after.json",
    "metrics_before.json",
    "metrics_after.json",
    "client/timeline.csv",
)


def _tar(dest: Path, files: list[Path], base: Path) -> None:
    if not files:
        return
    with tarfile.open(dest, "w:gz") as tar:
        for path in sorted(files):
            tar.add(path, arcname=str(path.relative_to(base)))


def archive_run(run_dir: Path, dest_root: Path) -> Path:
    if not (run_dir / "run.json").exists():
        raise FileNotFoundError(f"not a finished run directory: {run_dir}")
    dest = dest_root / run_dir.name
    if dest.exists():
        shutil.rmtree(dest)
    (dest / "client").mkdir(parents=True)
    for name in PLAIN:
        if (run_dir / name).exists():
            shutil.copy2(run_dir / name, dest / name)
    latencies = run_dir / "client" / "latencies.csv"
    if latencies.exists():
        with (
            latencies.open("rb") as src,
            gzip.open(dest / "client" / "latencies.csv.gz", "wb") as out,
        ):
            shutil.copyfileobj(src, out)
    _tar(dest / "logs.tar.gz", list((run_dir / "logs").glob("*.jsonl")), run_dir)
    _tar(
        dest / "client" / "histories.tar.gz",
        [*run_dir.glob("client/history-*.jsonl"), *run_dir.glob("client/e3_observations.jsonl")],
        run_dir,
    )
    return dest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--dest", type=Path, default=ROOT / "experiments" / "results" / "f1")
    args = parser.parse_args(argv)
    for run_dir in args.runs:
        print(archive_run(run_dir, args.dest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
