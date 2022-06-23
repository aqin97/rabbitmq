package rabbitmq

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	channel  *amqp.Channel
	Name     string
	exchange string
}

func New(url string) *RabbitMQ {
	conn, err := amqp.Dial(url)
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	mq := new(RabbitMQ)
	mq.channel = ch
	mq.Name = q.Name
	return mq
}

func (q *RabbitMQ) Bind(exchange string) {
	err := q.channel.QueueBind(
		q.Name,
		"",
		exchange,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	q.exchange = exchange
}

func (q *RabbitMQ) Send(queue string, data interface{}) {
	str, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	err = q.channel.Publish(
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			ReplyTo: q.Name,
			Body:    str,
		},
	)
	if err != nil {
		panic(err)
	}
}

func (q *RabbitMQ) Publish(exchange string, data interface{}) {
	str, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	err = q.channel.Publish(
		exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ReplyTo: q.Name,
			Body:    str,
		},
	)
	if err != nil {
		panic(err)
	}
}

func (q *RabbitMQ) Consume() <-chan amqp.Delivery {
	msgs, err := q.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	return msgs
}

func (q *RabbitMQ) Close() {
	q.channel.Close()
}
