package factory

import (
	"context"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	*baseMiddleware
	q amqp.Queue
}

func NewQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	qm := new(QueueMiddleware)
	base, err := newBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}
	qm.baseMiddleware = base
	qm.q, err = qm.ch.QueueDeclare(
		queueName, // name
		false,     // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,
	)
	if err != nil {
		qm.close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	err = qm.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		qm.close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	return qm, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if qm.isDisconnected() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return qm.consume(qm.q.Name, callbackFunc)
}

func (qm *QueueMiddleware) StopConsuming() error {
	return qm.stop()
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	if qm.isDisconnected() {
		return m.ErrMessageMiddlewareDisconnected
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // cancel the publish operation if it takes longer than 5 seconds
	defer cancel()

	errPublish := qm.publish(msg, ctx, "", qm.q.Name)
	if errPublish != nil {
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	return qm.close()
}
