# API contract examples

`examples/` holds example request and response bodies of the Host API (spec #1).
The Host tests (`host/tests/`) and the Webapp tests (`webapp/tests/`) both read these files,
so a change to the API contract fails a test on both sides.

The job ID in the examples is `example-job-id`. A test replaces it with the real job ID.
