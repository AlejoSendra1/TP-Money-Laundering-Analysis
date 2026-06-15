package middleware

import (
	"context"
	"time"
)

type ExchangeMiddleware struct {
	*baseMiddleware
	keys      []string
	exchange  string
	queueName string
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings, queueName string) (Middleware, error) {
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
		"topic",  // type
		true,     // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		em.close()
		return nil, err
	}
	// Si se usa el exchange para enviar, no es necesario que sera durable
	durable := true
	if queueName == "" {
		durable = false
	}
	q, err := em.ch.QueueDeclare(
		queueName, // name
		durable,   // durability
		false,     // delete when unused
		true,      // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		em.close()
		return nil, err
	}
	em.queueName = q.Name

	return em, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if em.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
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
			return ErrMessageMiddlewareMessage
		}
	}

	return em.consume(em.queueName, callbackFunc)
}

func (em *ExchangeMiddleware) StopConsuming() error {
	return em.stop()
}

func (em *ExchangeMiddleware) Send(msg Message) error {
	return em.send(em.keys, msg)
}

func (em *ExchangeMiddleware) SendWithKeys(keys []string, msg Message) error {
	return em.send(keys, msg)
}

func (em *ExchangeMiddleware) send(keys []string, msg Message) error {
	if em.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range keys {
		err := em.publish(msg, ctx, em.exchange, key)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return em.close()
}

func (em *ExchangeMiddleware) BindToTopics(exchangeName string, topic string) error {
	// Ya lo hace mas arriba, es solo para cumplir con la interfaz
	return nil
}
