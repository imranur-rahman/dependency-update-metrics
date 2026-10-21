# Testing

This document explains which kinds of tests `dependency-metrics` has, which files cover each kind, and the research that guides them.

## Guiding research

- **Kanewala and Bieman (2014)**, *Testing Scientific Software: A Systematic Literature Review*, Information and Software Technology 56, 1219–1232, [doi:10.1016/j.infsof.2014.05.006](https://doi.org/10.1016/j.infsof.2014.05.006). The review lists five testing levels for scientific software: unit, integration, system, acceptance, and regression testing. It also names the main obstacle for tools like this one, the **oracle problem**: often nothing tells the tester the correct output. It lists techniques that work around it, such as simplified data, pseudo oracles, reference data sets, and metamorphic testing.
- **Eisty and Carver (2022)**, *Testing Research Software: A Survey*, Empirical Software Engineering 27, 138, [doi:10.1007/s10664-022-10184-9](https://doi.org/10.1007/s10664-022-10184-9). In this survey of research software developers, unit testing was the most used type, followed by integration and system testing, and most respondents found these three useful. The survey also notes that research software often lacks automated regression and acceptance tests.

The oracle problem matters here because nobody knows the "true" MTTU or MTTR of a real package. The tests therefore combine the testing levels above with three oracle techniques.

## Running the tests

```bash
source metrics_venv/bin/activate
pytest tests/ -q
```

The suite has 290 tests and runs offline in a few seconds. Every resolver receives its cache and HTTP client through its constructor, so tests inject fixture data and never call a registry, deps.dev, or OSV. CI runs the suite on Ubuntu, macOS, and Windows with Python 3.10, 3.11, and 3.12.

This document covers the Tool. The Host and the Webapp have their own tests:

| Part | Command | What the tests check |
|---|---|---|
| Host | `pytest host/tests` | The HTTP API through FastAPI's `TestClient`. `create_app` takes a fake analysis function and a fake Turnstile check, and the tests run the queue and the worker (`Worker.run_once()`) in the test process. They check the Comparison rules (1 to 5 packages, no duplicates, one ecosystem, the package name rules of each ecosystem in `test_input_checks.py`, and the date rules), that a failed package keeps the results of the other packages, and that each failure shows one of the six user messages while the log keeps the full error under the job ID. `test_job_events.py` reads the event stream while a worker in a second thread runs the Jobs. `test_abuse_protection.py` checks the three limits and their configuration, the minutes in the rate-limit message, that a reused result does not count against the per-IP limits, that the queue file never holds a plain IP address and loses each hash after the window, the client IP behind a trusted proxy, CORS, and `TurnstileVerifier` with a fake Siteverify call. `test_tool_analysis.py` runs the real Tool with a deps.dev client stub, from submit to result, also for an unknown package ("Package not found in deps.dev") and a window without a release ("No releases in this window"). `test_per_release.py` does the same in Per-release mode, with one failed release whose row shows only the user message. `test_job_lifecycle.py` checks cancel, the timeout, the memory limit, and the worker pool. `test_result_reuse.py` checks the 72-hour result reuse (each key field, the OSV index date, package names that differ only in order, case, or PyPI separators, no reuse of a cancelled Job or of a Job with a server-side package or release error, and reuse of a Job whose only errors come from the user input) and the 30-day retention, with a fake clock in `JobQueue(clock=)`. Its isolated workers (`JobLimits`) run each Job in a child process, with module-level fake analysis functions and short limits. `test_health.py` checks `GET /api/health`: `ok` when the queue answers, and `503` with no detail when the queue file is gone. `test_ballast.py` checks the ballast sizing (25% of the RAM, only the gap to the use of the rest of the VM, never below 0) and a `Ballast` that reads /proc/meminfo text from an injected reader, then holds and releases real memory. `test_maintenance.py` checks the weekly maintenance: the OSV rebuild, then `--cache-prune-gb 10`, and, through the real Tool CLI, that a failed download keeps the old index and reports the failure. The deploy files in `host/` (Compose, Caddy, the scripts) get `docker compose config`, `bash -n`, and a manual check on the VM. |
| Webapp | `node --test webapp/tests/*.test.mjs` | The logic module `webapp/site/static/js/comparison.js` (form to request body, the package rules and the package name rules, the date rules, the message of a refused submit (also the shared rate-limit and busy examples), the bot check rule, the half-life rule, the axis label and the results note, the round trip between a Comparison and the page URL, Job status text and progress, when the user can cancel a Job, the job ID in the page URL, the bar chart data, the Per-release line data with gaps for failed releases, result table rows with a failed package, and the CSV text) and the published folder (local assets, Chart.js from cdnjs with an exact version, the Turnstile widget, and the privacy note), and `webapp/configure.mjs` (the Pages step that puts the production Host URL and site key into the homepage, and refuses a missing, non-HTTPS, or malformed value). The Chart.js drawing and the page layout get a manual smoke check. |

Both read the shared API examples in `contract/examples/`. The done examples are the real Tool output: `job_done_window_summary.json` for the scenario in `host/tests/test_tool_analysis.py`, and `job_done_per_release.json` (with one failed release) for the scenario in `host/tests/test_per_release.py`. Run that test with `DM_REGENERATE_CONTRACT=1` to write the Per-release example again. So the contract matches what the Tool returns. The done example with a package error (`job_done_with_package_error.json`) is the real output of `test_package_that_deps_dev_does_not_know_shows_package_not_found`.

## Coverage by test type

| Type | What it checks | Where |
|---|---|---|
| Unit | One component in isolation, with injected fixture data | `test_depsdev_resolver.py`, `test_depsdev_client.py`, `test_npm_resolver.py`, `test_pypi_resolver.py`, `test_pypi_version_resolution.py`, `test_crates_resolver.py`, `test_npm_semver.py`, `test_resolver_cache.py`, `test_cache_config.py`, `test_osv.py`, `test_severity_breakdown.py`, `test_time_utils.py`, `test_models.py`, `test_reporting.py`, `test_basic.py` |
| Integration | Components working together: the analyzer over stubbed resolvers, the public `analyze()` function over an injected fixture resolver or deps.dev client stub (Window summary and Per-release results, plain-data output, errors, a Per-release release that fails, and progress events), and the real `cli.main()` with stubbed resolvers and an in-process executor (`in_process_executor` fixture in `conftest.py`) | `test_per_release.py`, `test_dependency_first_use.py`, `test_api.py`, `test_bulk_csv.py`, `test_cli_resume.py`, `test_cli_progress.py`, `test_cli_single_package.py` |
| System | The whole tool, end to end, on 90 real packages (npm, PyPI, Cargo) with live data | The case study in `case_study/` (see below); run by hand, not in CI |
| Regression | Each bug found in real use has a test that fails on the old code | See the table below |
| Acceptance | — | Not automated. The authors are also the main users, which the review names as the usual reason acceptance testing is rare in research software. |

### Regression tests

| Bug (fix commit) | Tests |
|---|---|
| Per-release frames replayed overlapping timelines (`9e3beac`) | `test_per_release.py::test_dependency_frame_is_single_timeline_with_consistent_package_version`, `test_per_release.py::test_dependency_frame_prefers_highest_semver_over_most_recent_release`, `test_dependency_first_use.py` |
| Remediation always passed with deps.dev data (`04ae705`) | `test_osv.py::test_check_remediation_false_with_depsdev_shaped_metadata`, `test_osv.py::test_check_remediation_true_with_depsdev_shaped_metadata_when_fix_after_interval` |
| Cargo vulnerabilities did not match the OSV ecosystem name (`18cb57d`) | `test_bulk_csv.py::test_cargo_osv_ecosystem_name_matches_database_casing` |
| Cached version lists missed newer releases (`58f05ac`) | `*_refetches_when_end_date_exceeds_cached_versions` and `*_disk_cache_hit_when_end_date_already_covered` in `test_npm_resolver.py`, `test_pypi_resolver.py`, `test_crates_resolver.py`, `test_depsdev_client.py`, and `test_depsdev_resolver.py` |
| Scoped npm names created subdirectories in output paths (`26aabad`) | `test_reporting.py::test_scoped_npm_package_name_does_not_create_subdirectory`, `test_reporting.py::test_safe_filename_stem_replaces_path_separators` |
| Excel sheet titles with forbidden characters crashed the export (`189a64f`) | `test_reporting.py::test_safe_sheet_name_replaces_invalid_characters_and_truncates`, `test_reporting.py::test_export_worksheets_sanitizes_scoped_dependency_name`, `test_reporting.py::test_export_per_release_worksheets_sanitizes_scoped_dependency_name` |
| `--build-osv` did not rebuild an index that already existed (#13) | `test_osv.py::test_cli_build_osv_alone_rebuilds_existing_index` |
| Host: the API and a worker that opened a new Job queue file at once failed with "database is locked" (#4) | `host/tests/test_job_queue.py::test_api_and_workers_that_open_one_new_queue_file_at_once_all_succeed` |
| Host: several openers of an empty Job queue file with a stale `jobs.db-wal` failed with "disk I/O error" (`SQLITE_IOERR_DELETE_NOENT`), about 1 in 30 runs of the test above (#1) | `host/tests/test_job_queue.py::test_api_and_workers_that_open_an_empty_queue_file_with_a_stale_wal_all_succeed` |
| A failed deps.dev request became an empty version list or no dependencies, cached, and an MTTU of 0 (#1 review) | `test_depsdev_resolver.py::test_fetch_package_metadata_404_raises_package_not_found`, `test_depsdev_resolver.py::test_fetch_package_metadata_server_error_raises_data_source_error`, `test_depsdev_resolver.py::test_fetch_package_metadata_network_error_raises_data_source_error`, `test_depsdev_resolver.py::test_failed_fetch_package_metadata_is_not_cached`, `test_depsdev_resolver.py::test_extract_dependencies_raises_on_client_error`, `test_api.py::test_depsdev_unknown_package_*`, `test_api.py::test_depsdev_failed_dependency_lookup_*`, `test_api.py::test_depsdev_failed_manifest_*` |
| Host: the Host asked deps.dev a second time to tell an unknown package from a window without a release, and a failed dependency lookup showed an MTTU of 0 (#1 review) | `host/tests/test_tool_analysis.py::test_unknown_package_is_known_from_the_tool_outcome_without_a_second_request`, `host/tests/test_tool_analysis.py::test_failed_dependency_lookup_shows_something_went_wrong_not_a_zero` |
| Host: the Host reused a done Job in which a package or a release failed on the server side, for 72 hours (#1 review) | `host/tests/test_result_reuse.py::test_done_job_with_a_package_that_failed_on_the_server_is_not_reused`, `host/tests/test_result_reuse.py::test_done_job_with_a_failed_release_is_not_reused`, `host/tests/test_result_reuse.py::test_reuse_skips_a_newer_job_with_a_server_error_and_takes_an_older_clean_one` |
| Host: a Job that a worker claimed between two reads showed as queued with position 0, or as running with package 0 of 0 (#5) | `host/tests/test_job_events.py::test_stream_sends_the_states_in_order_with_position_and_package_index` |

### OSV build tests

The OSV build tests in `test_osv.py` write small fixture zips for npm, PyPI, and crates.io and inject a fake download function through the `fetch_zip` argument of `OSVBuilder`, so they run offline.

| What it checks | Test |
|---|---|
| The build from the three ecosystem zips gives the same index rows as the old build from the extracted `all.zip`. The expected rows come from the old code. | `test_osv_build_from_ecosystem_zips_matches_old_extracted_build` |
| The build extracts no files and leaves only the index in the output directory | `test_osv_build_does_not_extract_files_and_leaves_only_the_index` |
| A failed download keeps the old index | `test_failed_osv_download_keeps_old_index` |
| A write that stops partway keeps the old index, because the new index replaces it in one rename | `test_osv_index_write_that_fails_midway_keeps_old_index` |
| The index file holds its build date, and an index from an old build has no date | `test_osv_build_records_index_date`, `test_osv_index_date_is_none_for_index_from_old_build` |
| `--build-osv` without `--package` rebuilds an index that exists | `test_cli_build_osv_alone_rebuilds_existing_index` |

### Cache size and use tracking

`test_resolver_cache.py` covers the `last_used` column and the cache flags:

| Behavior | Tests |
|---|---|
| A cache file from before `last_used` opens and gets the column, with no data loss | `test_legacy_cache_file_gets_last_used_column_without_data_loss` |
| A prune keeps the most recently used rows and gets the used data below the limit | `test_prune_keeps_most_recently_used_rows_and_falls_below_limit`, `test_prune_under_limit_removes_nothing`, `test_prune_removes_legacy_rows_without_last_used_first` |
| A clear removes all rows, or only the rows of one namespace | `test_clear_without_namespace_removes_all_rows`, `test_clear_with_namespace_removes_only_that_namespace` |
| Stats show the rows and the size of each namespace | `test_cache_stats_shows_rows_and_bytes_per_namespace` |
| The CLI flags `--cache-stats`, `--cache-prune-gb`, `--cache-clear`, `--cache-namespace` | `test_cli_cache_*` |

## Working around the oracle problem

| Technique (Kanewala and Bieman) | How we use it | Where |
|---|---|---|
| Simplified data, so the correct output can be computed by hand | Tiny timelines with a known answer | `test_per_release.py::test_mttu_mttr_exponential_two_releases` (MTTU is exactly 365 days), `test_api.py::test_window_summary_returns_mttu_mttr_and_dependency_count` (579 days), `test_per_release.py::test_no_deps_returns_zero_metrics`, `test_metamorphic.py::test_source_scenario_has_nonzero_metrics` (214 days) |
| Metamorphic testing: check how the output must change when the input changes in a known way | Four relations, below | `test_metamorphic.py` |
| Pseudo oracle: compare against an independent implementation | The case study compares our scores with reference scores for the same packages and windows: 72 of 90 match exactly, and 82 are within one day. This counts as a pseudo oracle only if the reference scores came from an independent implementation. | `case_study/analyze_results.py` (columns `mttu_provided`, `mttu_recomputed`) |

### Metamorphic relations

Each test runs the analyzer on a source input and on a changed follow-up input, then checks the relation between the two outputs.

| Relation | Follow-up input | Expected output |
|---|---|---|
| MR1 | Add a vulnerability in a package that is not a dependency | MTTU and MTTR unchanged |
| MR2 | Add a vulnerability whose fixed version was never released | MTTR unchanged (the fix gate) |
| MR3 | Shift every date by 1, 365, or 1000 days | Unweighted MTTU and MTTR unchanged |
| MR4 | Add a dependency release that changes neither the selected nor the newest stable version | Unweighted MTTU and MTTR unchanged (the release only splits an interval) |

We checked that the relations can fail. A mutant that treats an unknown fix date as already published fails MR2. A mutant that removes the fix gate fails the source-scenario test.

## Adding a test

- Inject data through constructors (`ResolverCache`, `DepsDevClient`, `DependencyAnalyzer(resolver=...)`) or patch the resolver methods. Keep every test offline; `test_metamorphic.py` shows a fixture that blocks the network.
- Turn each bug fix into a regression test that fails on the old code, and add it to the regression table above.
- For a change to the metric logic, add a known-answer test or a metamorphic relation; an exact expected value for a real package is usually not available.
