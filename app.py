"""
Philippine Weather Streaming Dashboard
Pipeline:  WeatherAPI -> Kafka -> Spark Structured Streaming -> MongoDB -> Streamlit
Tab 1 (Live):     consumes the Kafka topic directly and refreshes every N seconds.
Tab 2 (History):  queries MongoDB (written by Spark), supports filters, aggregation
                  (raw / hourly / daily / weekly) and export to Excel, PDF and CSV.
"""

import io
import json
import os
from datetime import datetime, timedelta, timezone

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from fpdf import FPDF
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()

st.set_page_config(
    page_title="Weather Streaming Dashboard",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded",
)

LOCAL_TZ = "Asia/Manila"
MAX_BUFFER = 1000        # live records kept in memory per browser session
BACKFILL = 100           # messages per partition to load when the live view first connects
QUERY_LIMIT = 100_000    # max documents pulled from MongoDB per query

METRICS = {
    # key: (label, unit shown on charts, plain-text unit for PDF)
    "temperature": ("Temperature", "°C", "deg C"),
    "humidity": ("Humidity", "%", "%"),
    "pressure": ("Pressure", "hPa", "hPa"),
    "wind": ("Wind speed", "kph", "kph"),
}

TIME_RANGES = {
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "All": None,
}

AGG_FREQ = {"hourly": "1h", "daily": "1D", "weekly": "W"}


# --------------------------------------------------------------------------- UI helpers
def inject_css():
    st.markdown(
        """
        <style>
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #dbe3ee;
            border-radius: 12px;
            padding: 12px 16px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        }
        .pipeline-chip {
            display:inline-block; padding:2px 10px; margin-right:4px;
            border-radius:999px; background:#e1eefa; color:#0b4f78;
            font-size:0.82rem; font-weight:600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def metric_label(metric):
    label, unit, _ = METRICS.get(metric, (metric.title(), "", ""))
    return f"{label} ({unit})" if unit else label


def setup_sidebar():
    st.sidebar.title("⚙️ Dashboard Controls")

    st.sidebar.subheader("Kafka (live stream)")
    kafka_broker = st.sidebar.text_input("Kafka broker", value=os.getenv("KAFKA_BROKER", "localhost:9092"))
    kafka_topic = st.sidebar.text_input("Kafka topic", value=os.getenv("KAFKA_TOPIC", "weather-data"))

    st.sidebar.subheader("MongoDB (history)")
    mongo_uri = st.sidebar.text_input("Mongo URI", value=os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    mongo_db = st.sidebar.text_input("Database", value=os.getenv("MONGO_DB", "weather"))

    st.sidebar.subheader("Refresh")
    auto_refresh = st.sidebar.checkbox("Auto refresh live view", value=True)
    refresh_interval = st.sidebar.slider("Interval (seconds)", 5, 60, 15, disabled=not auto_refresh)
    if st.sidebar.button("🔄 Refresh now"):
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.caption(f"Times shown in {LOCAL_TZ} (UTC+8)")

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "mongo_uri": mongo_uri,
        "mongo_db": mongo_db,
        "auto_refresh": auto_refresh,
        "refresh_interval": refresh_interval,
    }


# --------------------------------------------------------------------------- Kafka (live)
def get_consumer(broker, topic):
    """Return (consumer, error_message). The consumer is cached per browser session."""
    key = f"kafka_consumer::{broker}::{topic}"
    consumer = st.session_state.get(key)
    if consumer is not None:
        return consumer, None

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[broker],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=False,
            api_version_auto_timeout_ms=3000,
        )
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            return None, f"Topic '{topic}' not found yet. Start producer.py and it will appear."

        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        ends = consumer.end_offsets(tps)
        for tp in tps:  # start a little before the end so the chart is not empty
            consumer.seek(tp, max(0, ends[tp] - BACKFILL))

        st.session_state[key] = consumer
        return consumer, None
    except NoBrokersAvailable:
        return None, f"Cannot reach Kafka at {broker}. Is Docker running? (docker compose up -d)"
    except KafkaError as e:
        return None, f"Kafka error: {e}"
    except Exception as e:
        return None, f"Unexpected error connecting to Kafka: {e}"


def consume_kafka_data(config):
    """Poll Kafka, append new messages to the session buffer, return (DataFrame, error)."""
    buffer = st.session_state.setdefault("live_buffer", [])
    broker, topic = config["kafka_broker"], config["kafka_topic"]

    consumer, error = get_consumer(broker, topic)
    if consumer:
        try:
            for _ in range(2):
                batches = consumer.poll(timeout_ms=1500, max_records=500)
                if not batches:
                    break
                for messages in batches.values():
                    for message in messages:
                        data = message.value
                        if isinstance(data, dict) and all(
                            k in data for k in ("timestamp", "value", "metric_type", "sensor_id")
                        ):
                            buffer.append(data)
        except Exception as e:  # broker went away mid-stream: drop consumer so it reconnects
            st.session_state.pop(f"kafka_consumer::{broker}::{topic}", None)
            error = f"Lost connection to Kafka: {e}"
        del buffer[:-MAX_BUFFER]

    if not buffer:
        return pd.DataFrame(), error

    df = pd.DataFrame(buffer)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["timestamp", "value"]).sort_values("timestamp").reset_index(drop=True)
    return df, error


# --------------------------------------------------------------------------- MongoDB (history)
@st.cache_resource
def get_mongo_client(uri):
    return MongoClient(uri, serverSelectionTimeoutMS=3000)


def build_filter(time_field, time_range, metrics, cities):
    query = {}
    delta = TIME_RANGES.get(time_range)
    if delta:
        query[time_field] = {"$gte": datetime.now(timezone.utc) - delta}
    if metrics:
        query["metric_type"] = {"$in": list(metrics)}
    if cities:
        query["sensor_id"] = {"$in": list(cities)}
    return query


@st.cache_data(ttl=10, show_spinner=False)
def _fetch(uri, db, collection, time_field, time_range, metrics, cities, limit):
    cursor = (
        get_mongo_client(uri)[db][collection]
        .find(build_filter(time_field, time_range, metrics, cities), {"_id": 0})
        .sort(time_field, -1)
        .limit(limit)
    )
    return list(cursor)


@st.cache_data(ttl=30, show_spinner=False)
def _distinct_cities(uri, db):
    return sorted(get_mongo_client(uri)[db]["readings"].distinct("sensor_id"))


def list_cities(config):
    try:
        return _distinct_cities(config["mongo_uri"], config["mongo_db"])
    except PyMongoError:
        return []


def query_historical_data(config, time_range="24h", metrics=None, cities=None):
    """Read raw readings from MongoDB. Returns (DataFrame, error_message)."""
    try:
        rows = _fetch(
            config["mongo_uri"], config["mongo_db"], "readings", "timestamp",
            time_range, tuple(metrics or ()), tuple(cities or ()), QUERY_LIMIT,
        )
    except PyMongoError as e:
        return pd.DataFrame(), f"Cannot connect to MongoDB at {config['mongo_uri']} ({type(e).__name__})."

    df = pd.DataFrame(rows)
    if df.empty:
        return df, None
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(LOCAL_TZ)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df, None


def query_spark_aggregates(config, time_range, metrics, cities):
    """Read the 1-minute windows computed by Spark."""
    try:
        rows = _fetch(
            config["mongo_uri"], config["mongo_db"], "aggregates_1min", "window_start",
            time_range, tuple(metrics or ()), tuple(cities or ()), 5000,
        )
    except PyMongoError:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ("window_start", "window_end"):
        df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(LOCAL_TZ)
    cols = ["window_start", "sensor_id", "metric_type", "unit", "avg_value", "min_value", "max_value", "readings"]
    return df[[c for c in cols if c in df.columns]].sort_values("window_start", ascending=False)


# --------------------------------------------------------------------------- Aggregation & export
def aggregate_data(df, aggregation):
    """raw -> unchanged; hourly/daily/weekly -> mean/min/max/count per city+metric."""
    if aggregation == "raw" or df.empty:
        return df
    out = (
        df.groupby(["sensor_id", "metric_type", "unit", pd.Grouper(key="timestamp", freq=AGG_FREQ[aggregation])])["value"]
        .agg(avg_value="mean", min_value="min", max_value="max", readings="count")
        .reset_index()
    )
    out = out[out["readings"] > 0]
    num_cols = ["avg_value", "min_value", "max_value"]
    out[num_cols] = out[num_cols].round(2)
    out["value"] = out["avg_value"]  # plotted column
    return out.sort_values("timestamp").reset_index(drop=True)


def summarize(df):
    return (
        df.groupby(["sensor_id", "metric_type", "unit"])["value"]
        .agg(readings="count", average="mean", minimum="min", maximum="max", std_dev="std")
        .round(2)
        .reset_index()
        .rename(columns={"sensor_id": "city", "metric_type": "metric"})
    )


@st.cache_data(show_spinner=False)
def build_excel(data, summary, filters):
    export = data.copy()
    export["timestamp"] = export["timestamp"].dt.tz_localize(None)
    first = ["timestamp", "sensor_id", "metric_type", "value", "unit"]
    export = export[first + [c for c in export.columns if c not in first]]

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        export.to_excel(writer, sheet_name="Data", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)
        pd.DataFrame(list(filters), columns=["Filter", "Value"]).to_excel(writer, sheet_name="Filters", index=False)
        for ws in writer.book.worksheets:
            ws.freeze_panes = "A2"
            for col in ws.columns:
                width = max(len(str(c.value)) if c.value is not None else 0 for c in col[:200]) + 2
                ws.column_dimensions[col[0].column_letter].width = min(width, 40)
    return out.getvalue()


def _ascii(text):
    """FPDF core fonts only support latin-1."""
    return str(text).encode("latin-1", "replace").decode("latin-1")


@st.cache_data(show_spinner=False)
def build_pdf(data, summary, filters):
    plot_df = data.copy()
    plot_df["timestamp"] = plot_df["timestamp"].dt.tz_localize(None)
    metrics = [m for m in METRICS if m in set(plot_df["metric_type"])] or sorted(set(plot_df["metric_type"]))

    fig, axes = plt.subplots(len(metrics), 1, figsize=(8, 2.4 * len(metrics)), squeeze=False)
    for ax, metric in zip(axes[:, 0], metrics):
        for city, g in plot_df[plot_df["metric_type"] == metric].groupby("sensor_id"):
            ax.plot(g["timestamp"], g["value"], label=city, linewidth=1.4)
        ax.set_title(metric_label(metric), fontsize=10)
        ax.grid(alpha=0.3)
        ax.legend(fontsize=7, ncol=3, loc="upper right")
    fig.autofmt_xdate()
    fig.tight_layout()
    chart = io.BytesIO()
    fig.savefig(chart, format="png", dpi=150)
    plt.close(fig)
    chart.seek(0)

    pdf = FPDF(format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "Weather Streaming Report", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 6, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} ({LOCAL_TZ})", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    for name, value in filters:
        pdf.cell(0, 5, _ascii(f"{name}: {value}"), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 8, "Summary statistics", new_x="LMARGIN", new_y="NEXT")
    headers = ["City", "Metric", "Readings", "Average", "Min", "Max"]
    widths = [45, 30, 25, 30, 30, 30]
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(225, 238, 250)
    for h, w in zip(headers, widths):
        pdf.cell(w, 7, h, border=1, fill=True)
    pdf.ln(7)
    pdf.set_font("Helvetica", "", 9)
    for _, r in summary.iterrows():
        cells = [r["city"], r["metric"], int(r["readings"]), f"{r['average']:.2f}", f"{r['minimum']:.2f}", f"{r['maximum']:.2f}"]
        for c, w in zip(cells, widths):
            pdf.cell(w, 6, _ascii(c), border=1)
        pdf.ln(6)

    pdf.add_page()
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 8, "Trends", new_x="LMARGIN", new_y="NEXT")
    pdf.image(chart, x=10, w=190)
    return bytes(pdf.output())


# --------------------------------------------------------------------------- Views
def display_real_time_view(config):
    """Tab 1. Runs as a fragment so only this tab refreshes every N seconds."""
    st.header("📈 Live Weather Stream")

    with st.spinner("Reading from Kafka..."):
        df, error = consume_kafka_data(config)

    if error:
        (st.info if df.empty and "not found" in error else st.error)(error)

    if df.empty:
        st.info("Waiting for data. Start the producer with `python producer.py`.")
        return

    c1, c2 = st.columns([1, 2])
    metric = c1.selectbox("Metric", list(METRICS), format_func=metric_label, key="live_metric")
    cities = c2.multiselect("Cities (empty = all)", sorted(df["sensor_id"].unique()), key="live_cities")

    view = df[df["metric_type"] == metric]
    if cities:
        view = view[view["sensor_id"].isin(cities)]
    if view.empty:
        st.warning(f"No {metric} readings in the live buffer yet.")
        return

    latest_ts = df["timestamp"].max()
    age = (datetime.now(timezone.utc) - latest_ts.to_pydatetime()).total_seconds()
    dot = "🟢" if age < 90 else "🟡" if age < 300 else "🔴"
    local_ts = latest_ts.tz_convert(LOCAL_TZ).strftime("%H:%M:%S")
    st.caption(
        f"{dot} Latest reading {local_ts} ({age:.0f}s ago) · {len(df)} messages buffered · "
        f"auto-refresh {'every ' + str(config['refresh_interval']) + 's' if config['auto_refresh'] else 'off'}"
    )

    # KPI cards: latest value per city with change since the previous reading
    latest_by_city = view.sort_values("timestamp").groupby("sensor_id")
    city_names = list(latest_by_city.groups)
    for start in range(0, len(city_names), 5):
        cols = st.columns(5)
        for col, city in zip(cols, city_names[start:start + 5]):
            g = latest_by_city.get_group(city)
            last = g["value"].iloc[-1]
            delta = f"{last - g['value'].iloc[-2]:+.2f}" if len(g) > 1 else None
            col.metric(city, f"{last:.1f} {METRICS.get(metric, ('', ''))[1]}", delta, delta_color="off")

    chart_df = view.copy()
    chart_df["timestamp"] = chart_df["timestamp"].dt.tz_convert(LOCAL_TZ)
    fig = px.line(
        chart_df, x="timestamp", y="value", color="sensor_id", markers=True,
        labels={"value": metric_label(metric), "timestamp": "Time", "sensor_id": "City"},
        template="plotly_white",
    )
    fig.update_layout(hovermode="x unified", margin=dict(t=30, b=10), legend_title_text="")
    st.plotly_chart(fig, width="stretch")

    with st.expander("📋 Latest raw messages"):
        recent = chart_df.sort_values("timestamp", ascending=False).head(50)
        st.dataframe(recent, width="stretch", hide_index=True)


def display_historical_view(config):
    st.header("📊 Historical Data")
    st.caption("Everything the pipeline has collected, read from MongoDB (written by the Spark job).")

    c1, c2, c3, c4 = st.columns(4)
    time_range = c1.selectbox("Time range", list(TIME_RANGES), index=1)
    metric_choice = c2.selectbox("Metric", ["all"] + list(METRICS), format_func=lambda m: "All metrics" if m == "all" else metric_label(m))
    aggregation = c3.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"])
    cities = c4.multiselect("Cities (empty = all)", list_cities(config))

    metrics = None if metric_choice == "all" else [metric_choice]
    raw, error = query_historical_data(config, time_range, metrics, cities)

    if error:
        st.error(error)
        st.info("Make sure Docker is running: `docker compose up -d`")
        return
    if raw.empty:
        st.warning("No data found for these filters. Check that the producer and the Spark job are running.")
        return

    data = aggregate_data(raw, aggregation)
    summary = summarize(raw)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Records", f"{len(raw):,}")
    k2.metric("Cities", raw["sensor_id"].nunique())
    k3.metric("From", raw["timestamp"].min().strftime("%b %d, %H:%M"))
    k4.metric("To", raw["timestamp"].max().strftime("%b %d, %H:%M"))
    if len(raw) >= QUERY_LIMIT:
        st.warning(f"Showing the newest {QUERY_LIMIT:,} records. Narrow the time range to see older data.")

    st.subheader("Trends" + ("" if aggregation == "raw" else f" ({aggregation} average)"))
    fig = px.line(
        data, x="timestamp", y="value", color="sensor_id",
        facet_row="metric_type" if data["metric_type"].nunique() > 1 else None,
        labels={"value": "Value", "timestamp": "Time", "sensor_id": "City"},
        template="plotly_white",
    )
    fig.update_yaxes(matches=None)
    fig.update_layout(hovermode="x unified", height=300 * max(1, data["metric_type"].nunique()), legend_title_text="")
    st.plotly_chart(fig, width="stretch")

    st.subheader("Summary statistics")
    st.dataframe(summary, width="stretch", hide_index=True)

    if data["metric_type"].nunique() == 1:
        bar = px.bar(
            summary, x="city", y="average", error_y=summary["maximum"] - summary["average"],
            error_y_minus=summary["average"] - summary["minimum"], template="plotly_white",
            labels={"city": "City", "average": f"Average {metric_label(metric_choice)}"},
        )
        st.plotly_chart(bar, width="stretch")

    with st.expander("⚡ Spark 1-minute aggregates"):
        agg = query_spark_aggregates(config, time_range, metrics, cities)
        if agg.empty:
            st.info("No Spark aggregates yet. They appear once the Spark job has processed a few minutes of data.")
        else:
            st.caption("Computed by Spark Structured Streaming with 1-minute event-time windows.")
            st.dataframe(agg, width="stretch", hide_index=True)

    st.subheader("All collected data")
    table = data.copy()
    table["timestamp"] = table["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    st.dataframe(table.sort_values("timestamp", ascending=False), width="stretch", hide_index=True, height=350)

    st.subheader("⬇️ Export")
    filters = (
        ("Time range", time_range),
        ("Metric", metric_choice),
        ("Aggregation", aggregation),
        ("Cities", ", ".join(cities) if cities else "all"),
        ("Records", str(len(raw))),
        ("Timezone", LOCAL_TZ),
    )
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    e1, e2, e3 = st.columns(3)
    e1.download_button(
        "📗 Excel (.xlsx)", build_excel(data, summary, filters), f"weather_{stamp}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    e2.download_button(
        "📕 PDF report", build_pdf(data, summary, filters), f"weather_report_{stamp}.pdf", "application/pdf",
    )
    e3.download_button(
        "📄 CSV", data.to_csv(index=False).encode("utf-8"), f"weather_{stamp}.csv", "text/csv",
    )


def main():
    inject_css()
    st.title("🌦️ Philippine Weather Streaming Dashboard")
    st.markdown(
        '<span class="pipeline-chip">WeatherAPI</span>→ <span class="pipeline-chip">Kafka</span>→ '
        '<span class="pipeline-chip">Spark</span>→ <span class="pipeline-chip">MongoDB</span>→ '
        '<span class="pipeline-chip">Streamlit</span>',
        unsafe_allow_html=True,
    )

    config = setup_sidebar()
    tab_live, tab_history = st.tabs(["📈 Live Stream", "📊 History"])

    with tab_live:
        run_every = f"{config['refresh_interval']}s" if config["auto_refresh"] else None
        st.fragment(run_every=run_every)(display_real_time_view)(config)

    with tab_history:
        display_historical_view(config)


if __name__ == "__main__":
    main()
