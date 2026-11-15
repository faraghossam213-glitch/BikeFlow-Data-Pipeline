CREATE TABLE IF NOT EXISTS live_aggregates (
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  station_id TEXT NOT NULL,
  avg_bikes DOUBLE PRECISION NOT NULL,
  min_bikes INTEGER NOT NULL,
  max_bikes INTEGER NOT NULL,
  event_count BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (window_start, window_end, station_id)
);
CREATE TABLE IF NOT EXISTS alerts (
  alert_id TEXT PRIMARY KEY,
  station_id TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  severity TEXT NOT NULL,
  message TEXT NOT NULL,
  available_bikes INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_live_window ON live_aggregates(window_start DESC);
CREATE INDEX IF NOT EXISTS idx_alert_time ON alerts(event_time DESC);
