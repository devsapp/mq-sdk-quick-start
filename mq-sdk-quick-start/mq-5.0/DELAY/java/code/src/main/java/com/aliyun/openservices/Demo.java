package com.aliyun.openservices;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.MessageBuilder;

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
    public static final String TAG = "${TAG}";
    public static final String KEY = "${KEY}";
    public static final String BODY = "${BODY}";
    public static final String DELAY_SECONDS = "${DELAY_SECONDS}";
    public static final String DELIVERY_TIMESTAMP = "${DELIVERY_TIMESTAMP}";

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

        /**
         * ${quickstart.provider.comment1}
         * ${quickstart.provider.comment2}
         * ${quickstart.provider.comment3}
         */
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .setTopics(TOPIC_NAME)
                .build();

        MessageBuilder builder = provider.newMessageBuilder()
                // ${quickstart.setTopic.comment}
                .setTopic(TOPIC_NAME)
                // ${quickstart.body.comment}
                .setBody(BODY.getBytes(StandardCharsets.UTF_8));

        if (!KEY.isEmpty()) {
            // ${quickstart.key.comment}
            builder.setKeys(KEY);
        }

        if (!TAG.isEmpty()) {
            // ${quickstart.tag.comment}
            builder.setTag(TAG);
        }

        if (!DELIVERY_TIMESTAMP.isEmpty()) {
            // ${quickstart.deliveryTimestamp.comment}
            builder.setDeliveryTimestamp(Long.parseLong(DELIVERY_TIMESTAMP));
        } else if (!DELAY_SECONDS.isEmpty()) {
            // ${quickstart.deliveryTimestamp.comment}
            builder.setDeliveryTimestamp(
                    System.currentTimeMillis() + Duration.ofSeconds(Long.parseLong(DELAY_SECONDS)).toMillis());
        }

        // ${quickstart.property.comment}
        ${quickstart.property.content}

        try {
            // ${quickstart.sendMessage.comment}
            final SendReceipt sendReceipt = producer.send(builder.build());
            System.out.println("Send mq message success! Topic is:" + TOPIC_NAME + " msgId is: "
                    + sendReceipt.getMessageId().toString());
        } catch (Throwable t) {
            System.out.println("Send mq message failed! Topic is:" + TOPIC_NAME);
            t.printStackTrace();
        }

        // ${quickstart.producer.close.comment}
        producer.close();
    }
}
