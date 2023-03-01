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
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;

public class Demo {
    /**
     * ${quickstart.endpoint.comment1}
     * ${quickstart.endpoint.comment2}
     * ${quickstart.endpoint.comment3}
     */
    public static final String ENDPOINT = "${ENDPOINT}";
    public static final String TOPIC_NAME = "${TOPIC_NAME}";
    public static final String FILTER_EXPRESSION = "${FILTER_EXPRESSION}";
    public static final String CONSUMER_GROUP_ID = "${CONSUMER_GROUP_ID}";
    public static final long AWAIT_DURATION_SECONDS = 10;
    public static final int MAX_MESSAGE = 16;
    public static final long INVISIBLE_DURATION_SECONDS = 15;

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder configBuilder = ClientConfiguration.newBuilder().setEndpoints(ENDPOINT);

        /**
         * ${quickstart.ak.comment1}
         * ${quickstart.ak.comment2}
         */
        // configBuilder.setCredentialProvider(new StaticSessionCredentialsProvider("Instance UserName", "Instance Password"));
        ClientConfiguration configuration = configBuilder.build();

        // ${quickstart.consumer.tag.comment}
        FilterExpression filterExpression = new FilterExpression(FILTER_EXPRESSION, FilterExpressionType.TAG);

        SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(configuration)
                // ${quickstart.consumer.group.comment}
                .setConsumerGroup(CONSUMER_GROUP_ID)
                // ${quickstart.consumer.topic.comment}
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC_NAME, filterExpression))
                // ${quickstart.consumer.awaitDuration.comment}
                .setAwaitDuration(Duration.ofSeconds(AWAIT_DURATION_SECONDS))
                .build();

        // ${quickstart.consumer.pulling.comment}
        do {
            final List<MessageView> messages = simpleConsumer.receive(
                    // ${quickstart.consumer.maxMessage.comment}
                    MAX_MESSAGE,
                    // ${quickstart.consumer.invisibleDuration.comment}
                    Duration.ofSeconds(INVISIBLE_DURATION_SECONDS));

            System.out.println("Received " + messages.size() + " message(s)");

            for (MessageView message : messages) {
                final MessageId messageId = message.getMessageId();
                try {
                    // ${quickstart.consumer.ack.comment}
                    simpleConsumer.ack(message);
                    System.out.println("Message is acknowledged successfully, messageId=" + messageId.toString());
                } catch (Throwable t) {
                    System.out.println("Message is failed to be acknowledged, messageId==" + messageId.toString());
                }
            }
        } while (true);

        // ${quickstart.consumer.simple.close.comment}
        // simpleConsumer.close();
    }
}
