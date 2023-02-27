package com.aliyun.openservices;

import java.util.Collections;
import java.io.IOException;

import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
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

        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // Set the consumer group name.
                .setConsumerGroup(CONSUMER_GROUP_ID)
                // Set the subscription for the consumer.
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC_NAME, filterExpression))
                .setMessageListener(messageView -> {
                    // Handle the received message and return consume result.
                    System.out.println("Consume message=" + messageView.toString());
                    return ConsumeResult.SUCCESS;
                })
                .build();
        // Block the main thread, no need for production environment.
        Thread.sleep(Long.MAX_VALUE);
        // Close the push consumer when you don't need it anymore.
        pushConsumer.close();
    }
}
