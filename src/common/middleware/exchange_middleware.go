package factory

import (
	"context"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

type ExchangeMiddleware struct {
	*baseMiddleware
	keys      []string
	exchange  string
	queueName string
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	em := new(ExchangeMiddleware)
	base, err := newBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}
	em.baseMiddleware = base

	em.keys = keys
	em.exchange = exchange
	err = em.ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		em.close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	q, err := em.ch.QueueDeclare(
		"",    // name
		false, // durability
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		em.close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}
	em.queueName = q.Name

	return em, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if em.isDisconnected() {
		return m.ErrMessageMiddlewareDisconnected
	}

	for _, key := range em.keys {
		// binding is idempotent, so "nothing" happen if this method is called repeatly
		err := em.ch.QueueBind(
			em.queueName, // queue name
			key,          // routing key
			em.exchange,  // exchange
			false,
			nil)

		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	return em.consume(em.queueName, callbackFunc)
}

func (em *ExchangeMiddleware) StopConsuming() error {
	return em.stop()
}

func (em *ExchangeMiddleware) Send(msg m.Message) error {
	if em.isDisconnected() {
		return m.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range em.keys {
		err := em.publish(msg, ctx, em.exchange, key)
		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return em.close()
}
