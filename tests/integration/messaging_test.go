//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gabrielAnFran/fiapx-saga-orchestrator/internal/infrastructure/messaging"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func uniqueSvc(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8]
}

func TestConn_DeclareServiceQueue_PublishAndConsume(t *testing.T) {
	conn, err := messaging.Dial(testAMQPURL)
	require.NoError(t, err)
	defer conn.Close()

	svc := uniqueSvc("declareq")
	qName, err := conn.DeclareServiceQueue(svc, []string{"video.uploaded"})
	require.NoError(t, err)
	assert.Equal(t, svc+".events.q", qName)

	ev, err := messaging.NewEvent("video.uploaded", "corr", "", map[string]string{"video_id": uuid.NewString()})
	require.NoError(t, err)
	require.NoError(t, conn.Publish(context.Background(), ev))

	msgs, err := conn.Channel().Consume(qName, "", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-msgs:
		var received messaging.Event
		require.NoError(t, json.Unmarshal(d.Body, &received))
		assert.Equal(t, "video.uploaded", received.EventName)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message on declared queue")
	}
}

func TestConn_Consume_HandlerSuccess_InvokesHandler(t *testing.T) {
	conn, err := messaging.Dial(testAMQPURL)
	require.NoError(t, err)
	defer conn.Close()

	svc := uniqueSvc("consumeok")
	_, err = conn.DeclareServiceQueue(svc, []string{"ConsumeOkEvent"})
	require.NoError(t, err)

	ev, err := messaging.NewEvent("ConsumeOkEvent", "corr", "", map[string]string{"k": "v"})
	require.NoError(t, err)
	require.NoError(t, conn.Publish(context.Background(), ev))

	received := make(chan messaging.Event, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = conn.Consume(ctx, svc, func(_ context.Context, e messaging.Event) error {
			received <- e
			return nil
		})
	}()

	select {
	case got := <-received:
		assert.Equal(t, "ConsumeOkEvent", got.EventName)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumed event")
	}
}

func TestConn_Consume_HandlerError_RoutesToRetryQueue(t *testing.T) {
	conn, err := messaging.Dial(testAMQPURL)
	require.NoError(t, err)
	defer conn.Close()

	svc := uniqueSvc("consumeerr")
	_, err = conn.DeclareServiceQueue(svc, []string{"ConsumeErrEvent"})
	require.NoError(t, err)

	ev, err := messaging.NewEvent("ConsumeErrEvent", "corr", "", map[string]string{"k": "v"})
	require.NoError(t, err)
	require.NoError(t, conn.Publish(context.Background(), ev))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		_ = conn.Consume(ctx, svc, func(context.Context, messaging.Event) error {
			return fmt.Errorf("boom")
		})
	}()

	retryCh, err := conn.Channel().Consume(svc+".retry.q", "", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-retryCh:
		assert.Equal(t, int32(1), d.Headers["x-retry-count"])
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message on retry queue")
	}
}

func TestConn_Consume_MalformedPayload_ParksWithoutInvokingHandler(t *testing.T) {
	conn, err := messaging.Dial(testAMQPURL)
	require.NoError(t, err)
	defer conn.Close()

	svc := uniqueSvc("malformed")
	_, err = conn.DeclareServiceQueue(svc, []string{"MalformedEvent"})
	require.NoError(t, err)

	require.NoError(t, conn.Channel().PublishWithContext(context.Background(), messaging.EventsExchange, "MalformedEvent", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte("not json"),
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var handlerCalls int
	go func() {
		_ = conn.Consume(ctx, svc, func(context.Context, messaging.Event) error {
			handlerCalls++
			return nil
		})
	}()

	retryCh, err := conn.Channel().Consume(svc+".retry.q", "", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-retryCh:
		assert.Equal(t, "not json", string(d.Body))
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for malformed message to be parked on the retry queue")
	}
	assert.Equal(t, 0, handlerCalls, "handler must never be invoked for an unparseable payload")
}

func TestConn_Retry_BelowMax_RoutesToRetryExchange(t *testing.T) {
	conn, err := messaging.Dial(testAMQPURL)
	require.NoError(t, err)
	defer conn.Close()

	svc := uniqueSvc("retrybelow")
	_, err = conn.DeclareServiceQueue(svc, []string{"AnyEvent"})
	require.NoError(t, err)

	delivery := amqp.Delivery{
		ContentType: "application/json",
		Body:        []byte(`{"n":1}`),
		Headers:     amqp.Table{},
	}
	require.NoError(t, conn.Retry(context.Background(), svc, delivery))

	retryCh, err := conn.Channel().Consume(svc+".retry.q", "", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-retryCh:
		assert.Equal(t, int32(1), d.Headers["x-retry-count"])
		assert.Equal(t, `{"n":1}`, string(d.Body))
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message on retry queue")
	}
}

func TestConn_Retry_ExceedsMaxRetries_GoesToDLQ(t *testing.T) {
	conn, err := messaging.Dial(testAMQPURL)
	require.NoError(t, err)
	defer conn.Close()

	svc := uniqueSvc("retrydlq")
	_, err = conn.DeclareServiceQueue(svc, []string{"AnyEvent"})
	require.NoError(t, err)

	delivery := amqp.Delivery{
		ContentType: "application/json",
		Body:        []byte(`{"hello":"world"}`),
		Headers:     amqp.Table{"x-retry-count": int32(messaging.MaxRetries)},
	}
	require.NoError(t, conn.Retry(context.Background(), svc, delivery))

	dlqCh, err := conn.Channel().Consume(svc+".events.dlq", "", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-dlqCh:
		assert.Equal(t, `{"hello":"world"}`, string(d.Body))
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message on dlq")
	}
}
