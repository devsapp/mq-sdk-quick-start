package com.aliyun.openservices;

import java.util.Collections;
import java.util.List;
import java.io.IOException;
import java.time.Duration;

import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;

public class Demo {
    // Please replace the ACCESS_KEY and SECRET_KEY with your RocketMQ instance
    // username and password.
    public static final String ACCESS_KEY = "";
    public static final String SECRET_KEY = "";

    // Please enable the public endpoint in instance detail page and replace the
    // following ENDPOINT parameter if you want to access it via internet.
    public static final String ENDPOINT = "${ENDPOINT}";

    public static final String TOPIC_NAME = "${TOPIC_NAME}";
    public static final String FILTER_EXPRESSION = "${FILTER_EXPRESSION}";
    public static final String CONSUMER_GROUP_ID = "${CONSUMER_GROUP_ID}";

    // Await duration for long-polling.
    public static final long AWAIT_DURATION_SECONDS = 10;
    // Max message num for each long polling.
    public static final int MAX_MESSAGE = 16;
    // Message invisible duration after it is received.
    public static final long INVISIBLE_DURATION_SECONDS = 15;

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        String accessKey = ACCESS_KEY;
        String secretKey = SECRET_KEY;
        if (accessKey.isEmpty() && System.getenv("USERNAME") != null) {
            accessKey = System.getenv("USERNAME");
        }
        if (secretKey.isEmpty() && System.getenv("PASSWORD") != null) {
            secretKey = System.getenv("PASSWORD");
        }

        ClientServiceProvider provider = ClientServiceProvider.loadService();

        StaticSessionCredentialsProvider credentialsProvider = new StaticSessionCredentialsProvider(
                accessKey, secretKey);

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(ENDPOINT)
                .setCredentialProvider(credentialsProvider)
                .build();

        FilterExpression filterExpression = new FilterExpression(FILTER_EXPRESSION, FilterExpressionType.TAG);

        SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // Set the consumer group name.
                .setConsumerGroup(CONSUMER_GROUP_ID)
                // set await duration for long-polling.
                .setAwaitDuration(Duration.ofSeconds(AWAIT_DURATION_SECONDS))
                // Set the subscription for the consumer.
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC_NAME, filterExpression))
                .build();

        do {
            final List<MessageView> messages = simpleConsumer.receive(
                    MAX_MESSAGE,
                    Duration.ofSeconds(INVISIBLE_DURATION_SECONDS));
            System.out.println("Received " + messages.size() + " message(s)");
            for (MessageView message : messages) {
                final MessageId messageId = message.getMessageId();
                try {
                    simpleConsumer.ack(message);
                    System.out.println("Message is acknowledged successfully, messageId=" + messageId.toString());
                } catch (Throwable t) {
                    System.out.println("Message is failed to be acknowledged, messageId==" + messageId.toString());
                }
            }
        } while (true);

        // Close the push consumer when you don't need it anymore.
        // simpleConsumer.close();
    }
}
