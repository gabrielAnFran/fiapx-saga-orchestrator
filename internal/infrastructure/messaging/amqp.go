package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	EventsExchange = "video.events"
	RetryExchange  = "video.retry"
	MaxRetries     = 5
)

type Event struct {
	EventID       string          `json:"event_id"`
	EventName     string          `json:"event_name"`
	EventVersion  int             `json:"event_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	CorrelationID string          `json:"correlation_id"`
	SagaID        string          `json:"saga_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

func NewEvent(name string, correlationID, sagaID string, payload any) (Event, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	return Event{
		EventID:       uuid.New().String(),
		EventName:     name,
		EventVersion:  1,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		SagaID:        sagaID,
		Payload:       b,
	}, nil
}

type Conn struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func Dial(url string) (*Conn, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.ExchangeDeclare(EventsExchange, "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}
	if err := ch.ExchangeDeclare(RetryExchange, "topic", true, false, false, false, nil); err != nil {
		return nil, err
	}
	return &Conn{conn: conn, ch: ch}, nil
}

func (c *Conn) Close() error {
	_ = c.ch.Close()
	return c.conn.Close()
}

func (c *Conn) Channel() *amqp.Channel { return c.ch }

func (c *Conn) Publish(ctx context.Context, ev Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return c.ch.PublishWithContext(ctx, EventsExchange, ev.EventName, false, false, amqp.Publishing{
		ContentType:   "application/json",
		Body:          body,
		MessageId:     ev.EventID,
		CorrelationId: ev.CorrelationID,
		Headers:       amqp.Table{"x-correlation-id": ev.CorrelationID, "x-retry-count": int32(0)},
		DeliveryMode:  amqp.Persistent,
	})
}

func (c *Conn) DeclareServiceQueue(svc string, routingKeys []string) (string, error) {
	q := svc + ".events.q"
	retryQ := svc + ".retry.q"
	dlq := svc + ".events.dlq"

	if _, err := c.ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return "", err
	}
	if _, err := c.ch.QueueDeclare(retryQ, true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(30000),
		"x-dead-letter-exchange": EventsExchange,
	}); err != nil {
		return "", err
	}
	if err := c.ch.QueueBind(retryQ, svc+".retry", RetryExchange, false, nil); err != nil {
		return "", err
	}
	if _, err := c.ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
		return "", err
	}
	for _, rk := range routingKeys {
		if err := c.ch.QueueBind(q, rk, EventsExchange, false, nil); err != nil {
			return "", err
		}
	}
	if err := c.ch.QueueBind(q, svc+".retry", EventsExchange, false, nil); err != nil {
		return "", err
	}
	return q, nil
}

func (c *Conn) Retry(ctx context.Context, svc string, d amqp.Delivery) error {
	count := int32(0)
	if v, ok := d.Headers["x-retry-count"]; ok {
		switch n := v.(type) {
		case int32:
			count = n
		case int64:
			if n > math.MaxInt32 {
				count = math.MaxInt32
			} else if n < math.MinInt32 {
				count = math.MinInt32
			} else {
				count = int32(n)
			}
		}
	}
	count++
	if count > MaxRetries {
		return c.ch.PublishWithContext(ctx, "", svc+".events.dlq", false, false, amqp.Publishing{
			ContentType: d.ContentType,
			Body:        d.Body,
			Headers:     d.Headers,
		})
	}
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["x-retry-count"] = count
	return c.ch.PublishWithContext(ctx, RetryExchange, svc+".retry", false, false, amqp.Publishing{
		ContentType: d.ContentType,
		Body:        d.Body,
		Headers:     headers,
	})
}

func (c *Conn) Consume(ctx context.Context, svc string, handler func(context.Context, Event) error) error {
	msgs, err := c.ch.Consume(svc+".events.q", "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("consumer channel closed")
			}
			var ev Event
			if err := json.Unmarshal(d.Body, &ev); err != nil {
				slog.Error("bad event payload, parking to dlq", "error", err)
				_ = c.Retry(ctx, svc, d)
				_ = d.Ack(false)
				continue
			}
			if err := handler(ctx, ev); err != nil {
				slog.Warn("handler failed, retrying", "event", ev.EventName, "error", err)
				if rerr := c.Retry(ctx, svc, d); rerr != nil {
					slog.Error("failed to publish retry", "error", rerr)
				}
				_ = d.Ack(false)
				continue
			}
			_ = d.Ack(false)
		}
	}
}
