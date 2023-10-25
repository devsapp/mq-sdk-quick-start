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
	Endpoint = "${ENDPOINT}"
	Topic    = "${TOPIC_NAME}"
	Username = "${USERNAME}"
	Password = "${PASSWORD}"
	Tag      = "${TAG}"
	Key      = "${KEY}"
	Body     = "${BODY}"
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
			AccessKey:    Username,
			AccessSecret: Password,
		},
	},
		/**
		 * ${quickstart.transactionChecker.comment1}
		 * ${quickstart.transactionChecker.comment2}
		 */
		rmq_client.WithTransactionChecker(&rmq_client.TransactionChecker{
			Check: func(msg *rmq_client.MessageView) rmq_client.TransactionResolution {
				log.Printf("check transaction message: %v", msg)
				return rmq_client.COMMIT
			},
		}),
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

	// ${quickstart.transaction.start.comment}
	transaction := producer.BeginTransaction()

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

	// ${quickstart.transaction.property.comment}
	msg.AddProperty("OrderId", "xxx")

	// ${quickstart.property.comment}
	${quickstart.property.content}

	// ${quickstart.transaction.sendHalf.comment}
	resp, err := producer.SendWithTransaction(context.TODO(), msg, transaction)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(resp); i++ {
		fmt.Printf("%#v\n", resp[i])
	}

	/**
	 * ${quickstart.transaction.result.comment1}
	 * ${quickstart.transaction.result.comment2}
	 * ${quickstart.transaction.result.comment3}
	 * ${quickstart.transaction.result.comment4}
	 *
	 */
	err = transaction.Commit()
	// err = transaction.RollBack()
	if err != nil {
		log.Fatal(err)
	}

}
