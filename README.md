# PulseLake

## The problem

Millions of people plan their day around a subway arrival estimate, and those estimates are only as trustworthy as the data pipeline behind them. New York City's MTA publishes live train positions and trip updates as an open GTFS-Realtime feed, but the raw form is a binary protobuf payload that expires within seconds, has no history, and gives no way to answer questions like "how late is the 6 line right now compared to twenty minutes ago" or "did we silently stop receiving data for the L train". PulseLake is a small, honest data pipeline that closes that gap: it polls the live MTA feed on a schedule, lands every observation in a local analytical store with the time it was captured, transforms the raw records into a clean modeled table with data quality tests attached, and surfaces the result on a dashboard. The point is not the dashboard. The point is that every layer between the feed and the chart is inspectable, tested, and can be pointed at when something looks wrong.

## Status

Local MVP, in progress. See the build log below.

- [x] Step 1: project structure, virtual environment, dependencies
- [ ] Step 2: ingestion script (MTA GTFS-Realtime, protobuf decode)
- [ ] Step 3: local storage (DuckDB)
- [ ] Step 4: loop scheduler and logging
- [ ] Step 5: dbt model and data quality test
- [ ] Step 6: Streamlit dashboard
- [ ] Step 7: unit tests
- [ ] Step 8: full README with architecture diagram and next steps

## Layout

```
pulselake/
  ingest/       fetch the MTA feed, decode protobuf, write to DuckDB
  transform/    dbt project that models the raw data into clean tables
  dashboard/    Streamlit app that reads the modeled tables
  data/         the DuckDB file lives here (git ignored)
  logs/         ingestion logs (git ignored)
  tests/        pytest unit tests, no network required
```

## Setup

No API key and no account are needed. The MTA subway GTFS-Realtime feeds have
been fully open since 2023. The feed this project uses by default is:

```
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs
```

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```
