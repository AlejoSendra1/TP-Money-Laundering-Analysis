package middleware

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	*baseMiddleware
	q amqp.Queue
}

func NewQueueMiddleware(queueName string, connectionSettings ConnSettings) (*QueueMiddleware, error) {
	qm := new(QueueMiddleware)
	base, err := newBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}
	qm.baseMiddleware = base
	qm.q, err = qm.ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		qm.close()
		return nil, err
	}

	err = qm.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		qm.close()
		return nil, err
	}
	return qm, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if qm.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	return qm.consume(qm.q.Name, callbackFunc)
}

func (qm *QueueMiddleware) StopConsuming() error {
	return qm.stop()
}

func (qm *QueueMiddleware) Send(msg Message) error {
	if qm.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // cancel the publish operation if it takes longer than 5 seconds
	defer cancel()

	errPublish := qm.publish(msg, ctx, "", qm.q.Name)
	if errPublish != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) BindToTopics(exchangeName string, topic string) error {
	// Por ahi seria mas idiomatico pasar un arreglo de pares (exchangeName, topic)
	err := qm.baseMiddleware.ch.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durability
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	log.Printf("Binding queue %s to exchange %s with routing key %s",
		qm.q.Name, exchangeName, topic)
	err = qm.baseMiddleware.ch.QueueBind(
		qm.q.Name,    // queue name
		topic,        // routing key
		exchangeName, // exchange
		false,
		nil)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	return qm.close()
}

func (qm *QueueMiddleware) SendWithKeys(keys []string, msg Message) error {
	// Aca es innecesario las keys, es solo para cumplir con la interfaz
	return qm.Send(msg)
}
