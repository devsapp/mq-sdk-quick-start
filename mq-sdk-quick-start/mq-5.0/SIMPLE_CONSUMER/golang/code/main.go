package main

import (
	"context"
	"fmt"
	"log"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
)

/**
 * ${quickstart.endpoint.comment1}
 * ${quickstart.endpoint.comment2}
 * ${quickstart.endpoint.comment3}
 */
const (
	Endpoint          = "${ENDPOINT}"
	Topic             = "${TOPIC_NAME}"
	Username          = "${USERNAME}"
	Password          = "${PASSWORD}"
	FilterExpression  = "${FILTER_EXPRESSION}"
	ConsumerGroup     = "${CONSUMER_GROUP_ID}"
	AwaitDuration     = time.Second * 10
	MaxMessageNum     = ${MAX_MESSAGE_NUM}
	InvisibleDuration = time.Second * ${INVISIBLE_DURATION_SECONDS}
)

func main() {
	// ${quickstart.consumer.tag.comment}
	filterExpression := rmq_client.NewFilterExpression(FilterExpression)

	simpleConsumer, err := rmq_client.NewSimpleConsumer(&rmq_client.Config{
		Endpoint:      Endpoint,
		ConsumerGroup: ConsumerGroup,
		Credentials: &credentials.SessionCredentials{
			/**
			 * ${quickstart.ak.comment1}
			 * ${quickstart.ak.comment2}
			 */
			AccessKey:    Username,
			AccessSecret: Password,
		},
	},
		rmq_client.WithAwaitDuration(AwaitDuration),
		rmq_client.WithSubscriptionExpressions(map[string]*rmq_client.FilterExpression{
			// ${quickstart.consumer.topic.comment}
			Topic: filterExpression,
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	err = simpleConsumer.Start()
	if err != nil {
		log.Fatal(err)
	}

	defer simpleConsumer.GracefulStop()

	// ${quickstart.consumer.pulling.comment}
	for {
		fmt.Println("start receive message")
		mvs, err := simpleConsumer.Receive(
			context.TODO(),
			// ${quickstart.consumer.maxMessage.comment}
			MaxMessageNum,
			// ${quickstart.consumer.invisibleDuration.comment}
			InvisibleDuration)
		if err != nil {
			fmt.Println(err)
		}

		for _, mv := range mvs {
			// ${quickstart.consumer.ack.comment}
			err := simpleConsumer.Ack(context.TODO(), mv)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(mv)
			}
		}
	}
}
