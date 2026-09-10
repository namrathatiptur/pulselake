"""
PulseLake dashboard.

Reads the dbt marts, not the raw tables. That separation is the point: the
dashboard contains no business logic, no cleaning, and no delay calculation.
Every number on this page comes from a model that is version controlled and
covered by tests. If a figure here looks wrong, the fix belongs in dbt, and
the same fix then applies to anything else reading the warehouse.

Run it with:

    streamlit run dashboard/app.py

Chart design notes, since these were deliberate choices and not defaults:

  * Trains per line is a single measure across categories, so it is one
    colour, not ten. A categorical palette here would imply the lines mean
    something different from each other in this chart, and they do not.

  * Prediction drift is polar data: losing time against making up time,
    around a meaningful zero. That is the one case for a diverging scale, so
    it uses the blue and red poles with a neutral grey midpoint. The pair was
    checked for colour vision deficiency separation rather than eyeballed.

  * Both charts are horizontal bars. Line labels read left to right, and a
    vertical bar chart would either rotate them or squeeze them.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make the project root importable so `ingest` and `dashboard` resolve the
# same way whether Streamlit is launched from the root or from anywhere else.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import altair as alt
import pandas as pd
import streamlit as st

from dashboard import queries

# Palette. Roles rather than raw hex at the point of use, so the values live
# in one place. These are validated steps: the blue and red diverging pair
# clears colour vision deficiency separation against the light surface.
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
SURFACE = "#fcfcfb"
GRID = "#e6e5e1"

SERIES_BLUE = "#2a78d6"
DIVERGE_LOSS = "#d03b3b"      # losing time, the negative direction for a rider
DIVERGE_GAIN = "#2a78d6"      # making up time
NEUTRAL_MID = "#b9b8b3"

STATUS_GOOD = "#0ca30c"
STATUS_WARNING = "#fab219"
STATUS_CRITICAL = "#d03b3b"

REFRESH_SECONDS = 20


st.set_page_config(
    page_title="PulseLake",
    page_icon="M",
    layout="wide",
)


def chart_theme():
    """Shared Altair styling: recessive axes and grid, text in ink tokens."""
    return {
        "config": {
            "background": SURFACE,
            "view": {"stroke": "transparent"},
            "axis": {
                "labelColor": INK_SECONDARY,
                "titleColor": INK_SECONDARY,
                "labelFontSize": 12,
                "titleFontSize": 12,
                "domainColor": GRID,
                "tickColor": GRID,
                "gridColor": GRID,
                "titleFontWeight": "normal",
            },
            "legend": {
                "labelColor": INK_SECONDARY,
                "titleColor": INK_SECONDARY,
            },
            "title": {"color": INK_PRIMARY, "fontSize": 14, "fontWeight": 600},
        }
    }


alt.themes.register("pulselake", chart_theme)
alt.themes.enable("pulselake")


# Cached with a short time to live. The underlying data changes every twenty
# to thirty seconds, so caching for ten avoids hammering DuckDB on every
# widget interaction without ever showing something meaningfully old.
@st.cache_data(ttl=10, show_spinner=False)
def cached(name: str, *args):
    return getattr(queries, name)(*args)


def humanize_seconds(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    seconds = int(seconds)
    if seconds < 90:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 90:
        return f"{minutes}m"
    return f"{minutes // 60}h {minutes % 60}m"


def render_freshness_banner() -> None:
    """
    Data age, first thing on the page.

    Every other number here is meaningless if this one is bad, and a stale
    dashboard looks exactly like a quiet system unless something checks the
    clock. Status is shown with an icon and a sentence, never colour alone.
    """
    freshness = cached("load_freshness")
    staleness = cached("load_model_staleness")

    age = freshness["age_seconds"]
    model_lag = staleness["lag_seconds"]

    if age is None:
        st.error("No data has been ingested yet. Start the poller first.")
        return

    if age < 120:
        colour, icon, verdict = STATUS_GOOD, "OK", "Ingestion is live"
    elif age < 900:
        colour, icon, verdict = STATUS_WARNING, "WARN", "Ingestion may have stopped"
    else:
        colour, icon, verdict = STATUS_CRITICAL, "STALE", "Ingestion has stopped"

    model_note = ""
    if model_lag is not None and model_lag > 120:
        model_note = (
            f" &nbsp;|&nbsp; dbt models are <strong>{humanize_seconds(model_lag)}</strong> "
            f"behind the raw data. Run <code>dbt build</code> to refresh them."
        )

    st.markdown(
        f"""
        <div style="
            border-left: 4px solid {colour};
            background: #f6f5f2;
            padding: 0.6rem 0.9rem;
            border-radius: 4px;
            margin-bottom: 1rem;
            color: {INK_SECONDARY};
            font-size: 0.9rem;">
            <strong style="color:{colour}">{icon}</strong>
            &nbsp;{verdict}. Newest snapshot is
            <strong>{humanize_seconds(age)}</strong> old
            ({freshness['total_snapshots']} snapshots stored,
            {freshness['recent_errors']} ingestion errors in the last 15 min).
            {model_note}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_headline_numbers(lines: pd.DataFrame, drift: pd.DataFrame) -> None:
    """
    Four hero numbers. These are stat tiles rather than charts because each
    one is a single value, and a chart of a single value is decoration.
    """
    total_trains = int(lines["trains_running"].sum())
    total_delayed = int(lines["trains_delayed"].sum())
    losing = int(drift["trips_losing_time"].sum()) if not drift.empty else 0
    gaining = int(drift["trips_making_up_time"].sum()) if not drift.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Trains running", f"{total_trains}")
    col2.metric("Lines reporting", f"{len(lines)}")
    col3.metric(
        "Flagged delayed by MTA",
        f"{total_delayed}",
        help=(
            "Trains carrying a service alert whose text mentions a delay. "
            "This is the only place MTA states outright that a train is late."
        ),
    )
    col4.metric(
        "Trips losing time",
        f"{losing}",
        delta=f"{gaining} making up time",
        delta_color="off",
        help=(
            "Trips whose predicted arrivals have slipped more than 2 minutes "
            "later since we first observed them. Derived from stored history, "
            "not published by MTA."
        ),
    )


def chart_trains_per_line(lines: pd.DataFrame) -> alt.Chart:
    """
    Trains currently running on each line.

    One measure across categories, so one colour. Using ten hues here would
    imply the lines are being compared as different kinds of thing, when the
    only thing being compared is a count.
    """
    base = alt.Chart(lines).encode(
        y=alt.Y("route_id:N", title=None, sort=alt.SortField("route_id")),
        x=alt.X(
            "trains_running:Q",
            title="Trains running",
            axis=alt.Axis(tickMinStep=1),
        ),
        tooltip=[
            alt.Tooltip("route_id:N", title="Line"),
            alt.Tooltip("trains_running:Q", title="Trains running"),
            alt.Tooltip("trains_at_station:Q", title="At a station"),
            alt.Tooltip("trains_in_transit:Q", title="In transit"),
            alt.Tooltip("trains_arriving:Q", title="Arriving"),
            alt.Tooltip("trains_delayed:Q", title="Delay alerts"),
            alt.Tooltip("median_minutes_to_next_stop:Q", title="Median min to next stop"),
        ],
    )

    bars = base.mark_bar(
        color=SERIES_BLUE,
        cornerRadiusTopRight=4,
        cornerRadiusBottomRight=4,
        height=16,
    )

    # Direct labels rather than a legend. One series needs no legend, and the
    # value at the end of each bar removes a round trip to the axis.
    labels = base.mark_text(
        align="left", dx=6, color=INK_SECONDARY, fontSize=12
    ).encode(text="trains_running:Q")

    return (bars + labels).properties(height=28 * max(len(lines), 1) + 20)


def chart_prediction_drift(drift: pd.DataFrame) -> alt.Chart:
    """
    How many trips on each line are losing time against making up time.

    The first version of this chart plotted the median drift per line and was
    almost blank, because the median trip on every line sits at 0.0 minutes.
    That is a true fact and a useless chart: most trains run to plan, so the
    median measures the quiet majority and hides the entire story, which is
    in the tail.

    Counting the trips on each side of the line is the honest fix. It is
    genuinely polar data around a meaningful zero, which is the one case that
    calls for a diverging scale: red to the right for trips slipping later,
    blue to the left for trips recovering. The pair was checked for colour
    vision deficiency separation rather than picked by eye, and the direction
    is also carried by position and by the axis label, so colour is never the
    only channel.
    """
    # Reshape to one row per (line, side) with making up time as a negative
    # count, which is what puts the two groups on opposite sides of zero.
    losing = drift[["route_id", "trips_losing_time", "trips_observed",
                    "median_drift_minutes"]].copy()
    losing["side"] = "Losing time"
    losing["trips"] = losing["trips_losing_time"]

    gaining = drift[["route_id", "trips_making_up_time", "trips_observed",
                     "median_drift_minutes"]].copy()
    gaining["side"] = "Making up time"
    gaining["trips"] = -gaining["trips_making_up_time"]

    data = pd.concat(
        [
            losing[["route_id", "side", "trips", "trips_observed",
                    "median_drift_minutes"]],
            gaining[["route_id", "side", "trips", "trips_observed",
                     "median_drift_minutes"]],
        ],
        ignore_index=True,
    )
    data["trips_abs"] = data["trips"].abs()

    limit = max(2.0, float(data["trips"].abs().max()) * 1.25)

    base = alt.Chart(data).encode(
        y=alt.Y("route_id:N", title=None, sort=alt.SortField("route_id")),
        x=alt.X(
            "trips:Q",
            title="Number of trips (left: making up time, right: losing time)",
            scale=alt.Scale(domain=[-limit, limit]),
            # Both arms count upward from zero, so the axis shows magnitude
            # and the side carries the sign. A negative tick label here would
            # read as "minus four trips", which is not a thing.
            axis=alt.Axis(labelExpr="abs(datum.value)", tickMinStep=1),
        ),
        color=alt.Color(
            "side:N",
            title=None,
            scale=alt.Scale(
                domain=["Losing time", "Making up time"],
                range=[DIVERGE_LOSS, DIVERGE_GAIN],
            ),
            legend=alt.Legend(orient="top", direction="horizontal"),
        ),
        tooltip=[
            alt.Tooltip("route_id:N", title="Line"),
            alt.Tooltip("side:N", title="Direction of drift"),
            alt.Tooltip("trips_abs:Q", title="Trips"),
            alt.Tooltip("trips_observed:Q", title="Trips observed on this line"),
            alt.Tooltip("median_drift_minutes:Q", title="Median drift (min)"),
        ],
    )

    bars = base.mark_bar(cornerRadius=3, height=16)

    zero_rule = (
        alt.Chart(pd.DataFrame({"x": [0]}))
        .mark_rule(color=INK_SECONDARY, strokeWidth=1)
        .encode(x="x:Q")
    )

    # Direct labels are selective on purpose. Labelling every bar including
    # the ones showing one or two trips crowds the middle of the chart where
    # the two arms meet, and those are the values least worth reading
    # precisely. Bars of three or more get a number; the rest are legible
    # from the axis and the tooltip.
    labels_right = (
        base.transform_filter("datum.trips >= 3")
        .mark_text(align="left", dx=5, color=INK_SECONDARY, fontSize=11)
        .encode(text=alt.Text("trips_abs:Q"))
    )
    labels_left = (
        base.transform_filter("datum.trips <= -3")
        .mark_text(align="right", dx=-5, color=INK_SECONDARY, fontSize=11)
        .encode(text=alt.Text("trips_abs:Q"))
    )

    return (zero_rule + bars + labels_right + labels_left).properties(
        height=28 * max(drift["route_id"].nunique(), 1) + 40
    )


def render_charts(lines: pd.DataFrame, drift: pd.DataFrame) -> None:
    left, right = st.columns(2)

    with left:
        st.subheader("Trains per line")
        st.caption("Newest snapshot. Counts every train the feed is reporting.")
        st.altair_chart(chart_trains_per_line(lines), use_container_width=True)

    with right:
        st.subheader("Are trains losing time?")
        st.caption(
            "MTA does not publish a delay figure for the subway, so this is "
            "derived: how far each train's own predicted arrivals have slipped "
            "since we first saw them. Only possible because snapshots are kept."
        )
        if drift.empty or drift["trips_observed"].sum() == 0:
            st.info(
                "Not enough history yet. Drift needs the same stop observed in "
                "at least two snapshots, so let the poller run a few minutes "
                "and rerun dbt."
            )
        else:
            st.altair_chart(chart_prediction_drift(drift), use_container_width=True)


def render_train_table(trains: pd.DataFrame) -> None:
    st.subheader("Trains right now")

    col1, col2, col3 = st.columns([2, 2, 3])
    lines_available = sorted(trains["line"].dropna().unique().tolist())
    chosen_lines = col1.multiselect("Line", lines_available, default=[])
    chosen_direction = col2.radio(
        "Direction", ["All", "Northbound", "Southbound"], horizontal=True
    )
    only_delayed = col3.checkbox("Only trains with an MTA delay alert", value=False)

    view = trains
    if chosen_lines:
        view = view[view["line"].isin(chosen_lines)]
    if chosen_direction == "Northbound":
        view = view[view["direction"] == "N"]
    elif chosen_direction == "Southbound":
        view = view[view["direction"] == "S"]
    if only_delayed:
        view = view[view["status"] == "Delayed (MTA alert)"]

    st.caption(
        f"{len(view)} of {len(trains)} trains. "
        "A negative time to next stop means the train arrived moments ago and "
        "MTA has not yet dropped the stop from its prediction list."
    )

    st.dataframe(
        view[
            [
                "line",
                "direction",
                "status",
                "at_stop",
                "next_stop",
                "minutes_to_next_stop",
                "trip_id",
                "has_reused_trip_id",
            ]
        ],
        use_container_width=True,
        hide_index=True,
        height=380,
        column_config={
            "line": st.column_config.TextColumn("Line", width="small"),
            "direction": st.column_config.TextColumn("Dir", width="small"),
            "status": st.column_config.TextColumn("Status", width="medium"),
            "at_stop": st.column_config.TextColumn("At stop", width="small"),
            "next_stop": st.column_config.TextColumn("Next stop", width="small"),
            "minutes_to_next_stop": st.column_config.NumberColumn(
                "Min to arrival", format="%.1f", width="small"
            ),
            "trip_id": st.column_config.TextColumn("Trip ID", width="medium"),
            "has_reused_trip_id": st.column_config.CheckboxColumn(
                "Shared trip ID",
                help=(
                    "NYCT assigns this trip ID to more than one physical train "
                    "in this snapshot. Roughly 1 percent of rows."
                ),
                width="small",
            ),
        },
    )


def render_pipeline_health() -> None:
    """
    The pipeline's own vital signs.

    Tucked into an expander because a rider does not care, but present
    because this is a data engineering project and the honest answer to "how
    do you know the number is right" is a table like this one.
    """
    with st.expander("Pipeline health and data quality"):
        health = cached("load_pipeline_health", 60)
        staleness = cached("load_model_staleness")

        if health.empty:
            st.info("No snapshots recorded yet.")
            return

        col1, col2, col3, col4 = st.columns(4)
        col1.metric(
            "Median gap between snapshots",
            f"{health['seconds_since_previous_snapshot'].median():.0f}s",
            help="How often MTA published a new snapshot that we captured.",
        )
        col2.metric(
            "Avg ingest lag",
            f"{health['avg_ingest_lag_seconds'].mean():.1f}s",
            help="Time between MTA publishing a snapshot and us fetching it.",
        )
        col3.metric(
            "Rows with a shared trip ID",
            f"{health['pct_rows_with_reused_trip_id'].mean():.2f}%",
            help=(
                "Known upstream defect. Tracked rather than hidden. A sharp "
                "move means something changed at MTA."
            ),
        )
        col4.metric(
            "dbt models behind raw",
            humanize_seconds(staleness["lag_seconds"]),
            help="Ingestion runs on a loop. dbt runs when you run it.",
        )

        gaps = health[health["is_after_ingestion_gap"]]
        if not gaps.empty:
            st.warning(
                f"{len(gaps)} ingestion gap(s) longer than 5 minutes are present "
                f"in the stored history. Those are periods where the poller was "
                f"not running, and any trend spanning them is not continuous."
            )

        errors = cached("load_recent_errors", 10)
        if not errors.empty:
            st.markdown("**Recent ingestion errors**")
            st.dataframe(errors, use_container_width=True, hide_index=True)

        st.markdown("**Snapshot history**")
        st.dataframe(
            health,
            use_container_width=True,
            hide_index=True,
            height=260,
        )


def render_drift_leaders() -> None:
    leaders = cached("load_drift_leaders", 10)
    if leaders.empty:
        return
    with st.expander("Trips whose predictions have slipped the most"):
        st.caption(
            "Ranked by the median change in this trip's own predicted arrival "
            "times. A trip observed at only one or two stops is a weaker "
            "signal than one observed at twenty, which is why the stop count "
            "is shown next to it."
        )
        st.dataframe(
            leaders,
            use_container_width=True,
            hide_index=True,
            column_config={
                "trip_id": st.column_config.TextColumn("Trip ID"),
                "line": st.column_config.TextColumn("Line", width="small"),
                "direction": st.column_config.TextColumn("Dir", width="small"),
                "stops_observed": st.column_config.NumberColumn("Stops observed"),
                "drift_minutes": st.column_config.NumberColumn(
                    "Median drift (min)", format="%+.1f"
                ),
            },
        )


def main() -> None:
    st.title("PulseLake")
    st.caption(
        "Live NYC subway data, ingested every 30 seconds, modelled in dbt, "
        "and served from a local DuckDB file. Every figure below comes from a "
        "tested dbt model rather than from this page."
    )

    if not queries.database_exists():
        st.error(
            "No database found at data/pulselake.duckdb. Run the ingestion "
            "first:\n\n`python -m ingest.run_once`"
        )
        return

    try:
        queries.check_ready()
    except queries.ModelsNotBuilt as exc:
        st.error(
            f"The dbt models have not been built yet (missing: {exc}).\n\n"
            "Build them with:\n\n`cd transform && dbt build`"
        )
        return

    render_freshness_banner()

    lines = cached("load_line_activity")
    drift = cached("load_drift_by_line")
    trains = cached("load_current_trains")

    if lines.empty:
        st.warning("The models are built but hold no rows yet. Let the poller run.")
        return

    render_headline_numbers(lines, drift)
    st.divider()
    render_charts(lines, drift)
    st.divider()
    render_train_table(trains)
    render_drift_leaders()
    render_pipeline_health()

    with st.sidebar:
        st.header("About")
        st.markdown(
            "**PulseLake** is a local data pipeline over the MTA "
            "GTFS-Realtime subway feed.\n\n"
            "```\nMTA feed\n  -> Python ingestion\n  -> DuckDB raw tables\n"
            "  -> dbt models and tests\n  -> this dashboard\n```\n"
            "No API key, no cloud, no paid services."
        )
        st.divider()
        if st.button("Refresh now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.caption(
            "Data is cached for 10 seconds. The poller writes every 30 "
            "seconds, and dbt only refreshes when you run it."
        )


if __name__ == "__main__":
    main()
