package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	rmq_client "github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
)

const (
	Topic     = "test"
	Endpoint  = "rmq-cn-zpr33wujr0g.cn-hangzhou.rmq.aliyuncs.com:8080"
	AccessKey = "naN59KGG1Wcg1O7C"
	SecretKey = "mH39k1UyERPvMn66"
)

func main() {
	os.Setenv("mq.consoleAppender.enabled", "true")
	rmq_client.ResetLogger()
	// new producer instance
	producer, err := rmq_client.NewProducer(&rmq_client.Config{
		Endpoint: Endpoint,
		Credentials: &credentials.SessionCredentials{
			AccessKey:    AccessKey,
			AccessSecret: SecretKey,
		},
	},
		rmq_client.WithTopics(Topic),
	)
	if err != nil {
		log.Fatal(err)
	}
	// start producer
	err = producer.Start()
	if err != nil {
		log.Fatal(err)
	}
	// graceful stop producer
	defer producer.GracefulStop()

	for i := 0; i < 10; i++ {
		// new a message
		msg := &rmq_client.Message{
			Topic: Topic,
			Body:  []byte("this is a message : " + strconv.Itoa(i)),
		}
		// set keys and tag
		msg.SetKeys("a", "b")
		msg.SetTag("ab")
		// send message in sync
		resp, err := producer.Send(context.TODO(), msg)
		if err != nil {
			log.Fatal(err)
		}
		for i := 0; i < len(resp); i++ {
			fmt.Printf("%#v\n", resp[i])
		}
		// wait a moment
		time.Sleep(time.Second * 1)
	}
}
