package com.aliyun.openservices;

import java.util.Collections;
import java.io.IOException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
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
    public static final String USERNAME = "${USERNAME}";
    public static final String PASSWORD = "${PASSWORD}";
    public static final String FILTER_EXPRESSION = "${FILTER_EXPRESSION}";
    public static final String CONSUMER_GROUP_ID = "${CONSUMER_GROUP_ID}";

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
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

        // ${quickstart.consumer.tag.comment}
        FilterExpression filterExpression = new FilterExpression(FILTER_EXPRESSION, FilterExpressionType.TAG);

        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(configuration)
                // ${quickstart.consumer.group.comment}
                .setConsumerGroup(CONSUMER_GROUP_ID)
                // ${quickstart.consumer.topic.comment}
                .setSubscriptionExpressions(Collections.singletonMap(TOPIC_NAME, filterExpression))
                .setMessageListener(messageView -> {
                    // ${quickstart.consumer.result.comment}
                    System.out.println("Consume message=" + messageView.toString());
                    return ConsumeResult.SUCCESS;
                })
                .build();
        Thread.sleep(Long.MAX_VALUE);
        // ${quickstart.consumer.close.comment}
        // pushConsumer.close();
    }
}
