package com.aliyun.openservices;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
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
    public static final String USERNAME = "${USERNAME}";
    public static final String PASSWORD = "${PASSWORD}";
    public static final String CONSUMER_GROUP_ID = "${CONSUMER_GROUP_ID}";

    public static void main(String[] args) throws ClientException, IOException {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder configBuilder = ClientConfiguration.newBuilder().setEndpoints(ENDPOINT);

        /**
         * ${quickstart.ak.comment1}
         * ${quickstart.ak.comment2}
         */
        configBuilder.setCredentialProvider(
            new StaticSessionCredentialsProvider(USERNAME, PASSWORD)
        );

        ClientConfiguration configuration = configBuilder.build();

        AtomicLong receiveMsgCount = new AtomicLong();
        String tag = "retry-" + System.currentTimeMillis();
        receiveMessages(provider, configuration, receiveMsgCount, tag);
        sendMessages(provider, configuration, tag);
    }

    private static void sendMessages(ClientServiceProvider provider, ClientConfiguration clientConfiguration,
            String tag) throws ClientException {

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

        try {
            Message message = messageBuilder.setTopic(TOPIC_NAME)
                    // ${quickstart.tag.comment}
                    .setTag(tag)
                    // ${quickstart.body.comment}
                    .setBody(("messageBody - test - retry").getBytes())
                    .build();

            // ${quickstart.sendMessage.comment}
            SendReceipt sendReceipt = producer.send(message);
            System.out.println("Send success : " + sendReceipt.getMessageId());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void receiveMessages(ClientServiceProvider provider, ClientConfiguration clientConfiguration,
            AtomicLong receiveMsgCount, String tag) throws ClientException {
        // ${quickstart.consumer.tag.comment}
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // ${quickstart.consumer.group.comment}
                .setConsumerGroup(CONSUMER_GROUP_ID)
                // ${quickstart.consumer.topic.comment}
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC_NAME, filterExpression))
                .setMessageListener(messageView -> {
                    // ${quickstart.consumer.result.comment}
                    receiveMsgCount.getAndIncrement();
                    System.out.println("Received the message for the " + receiveMsgCount.get() + "th time: "
                            + messageView.toString());

                    // ${quickstart.consumer.retry.comment}
                    return ConsumeResult.FAILURE;
                })
                .build();
    }
}