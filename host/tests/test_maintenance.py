"""The weekly maintenance (ticket #16): rebuild the OSV index, then prune the ResolverCache."""

from pathlib import Path

from dmhost.maintenance import run_weekly_maintenance


def test_maintenance_rebuilds_the_osv_index_then_prunes_the_cache_to_10_gb(tmp_path: Path):
    commands = []

    def run(argv):
        commands.append(list(argv))
        return 0

    report = run_weekly_maintenance(tmp_path, run=run)

    assert commands == [
        ["dependency-metrics", "--build-osv", "--output-dir", str(tmp_path)],
        ["dependency-metrics", "--cache-prune-gb", "10", "--output-dir", str(tmp_path)],
    ]
    assert report.ok


def _run_tool_in_process(argv):
    """Run the Tool CLI in this process, with the exit code that the subprocess gets."""
    from unittest.mock import patch

    from dependency_metrics.cli import main

    with patch("sys.argv", list(argv)):
        try:
            main()
        except SystemExit as exc:
            return exc.code or 0
        except Exception:  # an uncaught error ends the subprocess with 1
            return 1
    return 0


def test_failed_rebuild_keeps_the_old_index_and_reports_failure(tmp_path: Path, monkeypatch):
    import pandas as pd

    from dependency_metrics import osv_builder

    old_index = pd.DataFrame(
        [
            {
                "vuln_id": "OLD-1",
                "ecosystem": "npm",
                "package": "old",
                "introduced": "0",
                "fixed": "1.0.0",
                "severity": "LOW",
            }
        ]
    )
    old_index.to_parquet(tmp_path / "osv_database.parquet", index=False)

    def download_fails(url, dest):
        raise RuntimeError("network down")

    monkeypatch.setattr(osv_builder, "download_file", download_fails)

    report = run_weekly_maintenance(tmp_path, run=_run_tool_in_process)

    assert not report.ok
    # The prune still runs, because the cache size does not depend on the index.
    assert report.failed_steps == ["osv rebuild"]
    pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / "osv_database.parquet"), old_index)
