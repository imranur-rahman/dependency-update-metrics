 # Last completed package count (shows X/Y progress)
  grep "Completed package" output/run.log | tail -1

  # Just the counter — e.g. "502/9004"
  grep "Completed package" output/run.log | tail -1 | grep -o '[0-9]*/[0-9]*'

  # All memory checkpoints (every 10 packages, shows RSS too)
  grep "Memory after" output/run.log | tail -20

  # Count of "Completed package" lines (= packages processed in this run)
  grep -c "Completed package" output/run.log

  The most useful one is usually:

  grep "Completed package" output/run.log | tail -1

  dependency-metrics --input-csv ./pypi_data.csv --weighting-type exponential --half-life 80 --workers 32 --per-release --severity-breakdown --resume

 dependency-metrics --input-csv ./package_data_npm_only.csv --weighting-type exponential --half-life 80 --workers 6 --per-release --severity-breakdown --resume