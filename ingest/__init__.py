"""
PulseLake ingestion package.

Modules are split so that the pure parsing logic can be unit tested without
touching the network or the database:

    config.py   shared paths, feed URLs, and tuning knobs
    parse.py    protobuf bytes in, plain Python dictionaries out (no I/O)
    fetch.py    HTTP calls to the MTA feed, with retries and error handling
    storage.py  DuckDB schema creation and idempotent inserts
    run_once.py fetch plus store, a single cycle
    run_loop.py the simple scheduler that calls run_once on an interval
"""
