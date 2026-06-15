package middleware

import (
	"context"
	"time"
)

// No hace falta que tenga una nombre la queue, porque el exchange solo se usa para enviar, no consumir
func NewDinamicExchangeMiddleware(exchange string, connectionSettings ConnSettings) (*ExchangeMiddleware, error) {
	em := new(ExchangeMiddleware)
	base, err := newBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}
	em.baseMiddleware = base

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

	q, err := em.ch.QueueDeclare(
		"",    // name
		true,  // durability
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		em.close()
		return nil, err
	}
	em.queueName = q.Name

	return em, nil
}

func (em *ExchangeMiddleware) SendToTopic(msg Message, key string) error {
	if em.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := em.publish(msg, ctx, em.exchange, key)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return nil
}
