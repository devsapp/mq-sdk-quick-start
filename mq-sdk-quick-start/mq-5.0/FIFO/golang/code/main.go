package main

import (
	"context"
	"fmt"
	"log"

	rmq_client "github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
)

/**
 * ${quickstart.endpoint.comment1}
 * ${quickstart.endpoint.comment2}
 * ${quickstart.endpoint.comment3}
 */
const (
	Endpoint     = "${ENDPOINT}"
	Topic        = "${TOPIC_NAME}"
	Tag          = "${TAG}"
	Key          = "${KEY}"
	Body         = "${BODY}"
	MessageGroup = "MESSAGE_GROUP"
)

func main() {
	/**
	 * ${quickstart.provider.comment1}
	 * ${quickstart.provider.comment2}
	 * ${quickstart.provider.comment3}
	 */
	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: Endpoint,
		Credentials: &credentials.SessionCredentials{
			/**
			 * ${quickstart.ak.comment1}
			 * ${quickstart.ak.comment2}
			 */
			AccessKey:    "",
			AccessSecret: "",
		},
	},
		rmq_client.WithTopics(Topic),
	)
	if err != nil {
		log.Fatal(err)
	}

	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}

	defer producer.GracefulStop()

	msg := &rmq_client.Message{
		// ${quickstart.setTopic.comment}
		Topic: Topic,
		// ${quickstart.body.comment}
		Body: []byte(Body),
	}

	if Key != "" {
		// ${quickstart.key.comment}
		msg.SetKeys(Key)
	}

	if Tag != "" {
		// ${quickstart.tag.comment}
		msg.SetTag(Tag)
	}

	if MessageGroup != "" {
		// ${quickstart.messageGroup.comment}
		msg.SetMessageGroup(MessageGroup)
	}

	// ${quickstart.property.comment}
	${quickstart.property.content}

	// ${quickstart.sendMessage.comment}
	resp, err := producer.Send(context.TODO(), msg)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(resp); i++ {
		fmt.Printf("%#v\n", resp[i])
	}
}
