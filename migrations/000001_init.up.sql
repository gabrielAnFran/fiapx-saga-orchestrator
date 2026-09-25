CREATE TABLE saga_instances (
  id UUID PRIMARY KEY,
  saga_type TEXT NOT NULL,
  video_id UUID NOT NULL,
  state TEXT NOT NULL,
  context JSONB NOT NULL,
  last_event_id UUID,
  retry_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX idx_saga_video ON saga_instances(video_id);
CREATE TABLE saga_history (
  id BIGSERIAL PRIMARY KEY,
  saga_id UUID NOT NULL REFERENCES saga_instances(id),
  from_state TEXT, to_state TEXT NOT NULL,
  event_name TEXT, event_id UUID,
  error TEXT, at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE outbox (
  id BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL UNIQUE, aggregate_id UUID NOT NULL,
  event_name TEXT NOT NULL, payload JSONB NOT NULL, headers JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox(created_at) WHERE published_at IS NULL;
CREATE TABLE processed_events (
  event_id UUID PRIMARY KEY, processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
