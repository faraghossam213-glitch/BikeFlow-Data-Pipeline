import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="BikeFlow | Live Operations", page_icon="🚲", layout="wide")
st.title("🚲 BikeFlow — City Bike Availability")
st.caption("Live station status, short-term supply, and data quality alerts")

@st.cache_data(ttl=8)
def query(sql, params=None):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"), 
        port=5432,
        dbname=os.getenv("DB_NAME", "bikeflow"), 
        user=os.getenv("DB_USER", "bikeflow"),
        password=os.getenv("DB_PASSWORD", "change_me_locally")
    )
    try:
        return pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

page = st.sidebar.radio("Page", ["Live", "History"])
max_hours = st.sidebar.slider("History range (hours)", 1, 168, 24)
station = st.sidebar.text_input("Station ID contains", "")
filter_sql = " AND station_id ILIKE %s" if station else ""
params = [f"%{station}%"] if station else None

if page == "Live":
    @st.fragment(run_every="10s")
    def live_view():
        data = query("SELECT * FROM live_aggregates WHERE window_end >= now() - interval '24 hours'" + filter_sql + " ORDER BY window_end DESC LIMIT 2000", params)
        alerts = query("SELECT * FROM alerts WHERE event_time >= now() - interval '24 hours'" + filter_sql + " ORDER BY event_time DESC LIMIT 50", params)
        if data.empty:
            st.info("Waiting for data. Keep the replayer running.")
            return
        a, b, c, d = st.columns(4)
        a.metric("Active stations", f"{data.station_id.nunique():,}")
        b.metric("Average bikes available", f"{data.avg_bikes.mean():.1f}")
        c.metric("Lowest station count", f"{int(data.min_bikes.min())}")
        d.metric("Alerts (24h)", f"{len(alerts):,}")
        left, right = st.columns(2)
        left.plotly_chart(px.line(data.sort_values("window_start"), x="window_start", y="avg_bikes", color="station_id", title="Average bikes by station and minute"), use_container_width=True)
        right.plotly_chart(px.bar(data.groupby("station_id", as_index=False).agg(events=("event_count", "sum")).sort_values("events", ascending=False).head(15), x="station_id", y="events", title="Most active stations"), use_container_width=True)
        st.subheader("Latest alerts")
        st.dataframe(alerts, use_container_width=True, hide_index=True)
    live_view()
else:
    hist = query("SELECT * FROM live_aggregates WHERE window_start >= now() - (%s || ' hours')::interval" + filter_sql + " ORDER BY window_start", [str(max_hours)] + (params or []))
    if hist.empty:
        st.info("No history available yet.")
    else:
        hist["day"] = pd.to_datetime(hist.window_start).dt.date
        daily = hist.groupby("day", as_index=False).agg(avg_bikes=("avg_bikes", "mean"), events=("event_count", "sum"), stations=("station_id", "nunique"))
        left, right = st.columns(2)
        left.plotly_chart(px.line(daily, x="day", y="avg_bikes", title="Daily average bikes available"), use_container_width=True)
        right.plotly_chart(px.bar(daily, x="day", y="events", title="Events processed by day"), use_container_width=True)
        st.dataframe(daily, use_container_width=True, hide_index=True)