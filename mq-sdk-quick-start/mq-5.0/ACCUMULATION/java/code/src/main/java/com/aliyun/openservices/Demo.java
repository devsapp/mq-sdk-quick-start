package com.aliyun.openservices;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.message.Message;

public class Demo {
    /**
     * ${quickstart.endpoint.comment1}
     * ${quickstart.endpoint.comment2}
     * ${quickstart.endpoint.comment3}
     */
    public static final String ENDPOINT = "${ENDPOINT}";
    public static final String TOPIC_NAME = "${TOPIC_NAME}";
    public static final String CONSUMER_GROUP_ID = "${CONSUMER_GROUP_ID}";

    public static void main(String[] args) throws ClientException, IOException {
        /**
         * ${quickstart.lag.print.count}
         */
        AtomicLong receiveMsgCount = new AtomicLong();
        AtomicLong sendMsgCount = new AtomicLong();
        Timer timer = new Timer();
        countMsgAndPrint(timer, sendMsgCount, receiveMsgCount);

        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder configBuilder = ClientConfiguration.newBuilder().setEndpoints(ENDPOINT);

        /**
         * ${quickstart.ak.comment1}
         * ${quickstart.ak.comment2}
         */
        // configBuilder.setCredentialProvider(
        //  new StaticSessionCredentialsProvider("Instance UserName", "Instance Password")
        // );

        ClientConfiguration configuration = configBuilder.build();

        /**
         * ${quickstart.lag.receive.messages}
         */
        receiveMessages(provider, configuration, receiveMsgCount);

        /**
         * ${quickstart.lag.send.messages}
         */
        sendMessages(provider, configuration, sendMsgCount);
    }

    private static void sendMessages(ClientServiceProvider provider, ClientConfiguration clientConfiguration,
            AtomicLong sendMsgCount) throws ClientException {

        /**
         * ${quickstart.provider.comment1}
         * ${quickstart.provider.comment2}
         * ${quickstart.provider.comment3}
         */
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopics(TOPIC_NAME)
                .build();

        MessageBuilder messageBuilder = provider.newMessageBuilder();

        while (true) {
            try {
                Message message = messageBuilder.setTopic(TOPIC_NAME)
                        // ${quickstart.body.comment}
                        .setBody(("messageBody - lag - test").getBytes())
                        .build();

                // ${quickstart.sendMessage.comment}
                producer.send(message);
                sendMsgCount.getAndIncrement();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void receiveMessages(ClientServiceProvider provider, ClientConfiguration clientConfiguration,
            AtomicLong receiveMsgCount) throws ClientException {
        Random random = new Random();

        // ${quickstart.consumer.tag.comment}
        FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);
        provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // ${quickstart.consumer.group.comment}
                .setConsumerGroup(CONSUMER_GROUP_ID)
                // ${quickstart.consumer.topic.comment}
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC_NAME, filterExpression))
                .setMessageListener(messageView -> {
                    // ${quickstart.consumer.result.comment}
                    try {
                        Thread.sleep(random.nextInt(10000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    receiveMsgCount.getAndIncrement();
                    return ConsumeResult.SUCCESS;
                })
                .build();
    }

    public static void countMsgAndPrint(Timer timer, AtomicLong sendMsgCount, AtomicLong receiveMsgCount) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                System.out.println(sdf.format(new Date()) + " , total sent message count : " + sendMsgCount.get()
                        + ", total received message count : " + receiveMsgCount.get());
            }
        };
        long period = 5 * 1000;
        timer.schedule(timerTask, period, period);
    }
}
