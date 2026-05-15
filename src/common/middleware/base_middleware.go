package factory

import (
	"context"
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	consumerTag string
}

func newBaseMiddleware(connectionSettings m.ConnSettings) (*baseMiddleware, error) {
	bm := new(baseMiddleware)

	addr := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	var err error

	bm.conn, err = amqp.Dial(addr)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	bm.ch, err = bm.conn.Channel()
	if err != nil {
		bm.conn.Close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	bm.consumerTag = ""

	return bm, nil
}

func (bm *baseMiddleware) isDisconnected() bool {
	return bm.conn == nil || bm.conn.IsClosed() || bm.ch == nil
}

func (bm *baseMiddleware) stop() error {
	if bm.consumerTag == "" {
		return nil
	}
	tag := bm.consumerTag
	bm.consumerTag = ""

	if bm.isDisconnected() {
		return m.ErrMessageMiddlewareDisconnected
	}
	err := bm.ch.Cancel(tag, false)

	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (bm *baseMiddleware) close() error {
	var errCh, errConn error
	if bm.ch != nil {
		errCh = bm.ch.Close()
	}
	if bm.conn != nil {
		errConn = bm.conn.Close()
	}
	bm.ch, bm.conn, bm.consumerTag = nil, nil, ""
	if errCh != nil || errConn != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

func (bm *baseMiddleware) consume(queueName string, callBackFunc func(msg m.Message, ack func(), nack func())) error {
	bm.consumerTag = fmt.Sprintf("consumer%d", time.Now().UnixNano())
	msgs, err := bm.ch.Consume(
		queueName,
		bm.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		msg := m.Message{Body: string(d.Body)}

		ack := func() {
			d.Ack(
				false, // only ack this message, not the ones before
			)
		}
		nack := func() {
			d.Nack(
				false, // only nack this message, not the ones before
				true,  // requeue this message instead of discarding it
			)
		}
		callBackFunc(msg, ack, nack)
	}

	return m.ErrMessageMiddlewareMessage // msgs is closed
}

func (bm *baseMiddleware) publish(msg m.Message, ctx context.Context, exchange string, key string) error {
	return bm.ch.PublishWithContext(
		ctx,
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
}
