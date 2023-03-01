package com.aliyun.openservices;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;

public class Demo {
    /**
     * ${quickstart.endpoint.comment1}
     * ${quickstart.endpoint.comment2}
     * ${quickstart.endpoint.comment3}
     */
    public static final String ENDPOINT = "${ENDPOINT}";
    public static final String TOPIC_NAME = "${TOPIC_NAME}";
    public static final String TAG = "${TAG}";
    public static final String KEY = "${KEY}";
    public static final String BODY = "${BODY}";

    public static void main(String[] args) throws ClientException, IOException {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder configBuilder = ClientConfiguration.newBuilder().setEndpoints(ENDPOINT);

        /**
         * ${quickstart.ak.comment1}
         * ${quickstart.ak.comment2}
         */
        // configBuilder.setCredentialProvider(new StaticSessionCredentialsProvider("Instance UserName", "Instance Password"));
        ClientConfiguration configuration = configBuilder.build();

        /**
         * ${quickstart.provider.comment1}
         * ${quickstart.provider.comment2}
         * ${quickstart.provider.comment3}
         */
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(configuration)
                .setTopics(TOPIC_NAME)
                /**
                 * ${quickstart.transactionChecker.comment1}
                 * ${quickstart.transactionChecker.comment2}
                 */
                .setTransactionChecker(messageView -> {
                    System.out.println("Receive transactional message check, message=" + messageView.toString());
                    return TransactionResolution.COMMIT;
                })
                .build();

        // ${quickstart.transaction.start.comment}
        final Transaction transaction = producer.beginTransaction();

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

        // ${quickstart.transaction.property.comment}
        builder.addProperty("OrderId", "xxx");

        try {
            // ${quickstart.transaction.sendHalf.comment}
            final SendReceipt sendReceipt = producer.send(builder.build(), transaction);
            System.out.println("Send mq message success! Topic is:" + TOPIC_NAME + " msgId is: "
                    + sendReceipt.getMessageId().toString());
        } catch (Throwable t) {
            System.out.println("Send mq message failed! Topic is:" + TOPIC_NAME);
            t.printStackTrace();
        }

        /**
         * ${quickstart.transaction.result.comment1}
         * ${quickstart.transaction.result.comment2}
         * ${quickstart.transaction.result.comment3}
         * ${quickstart.transaction.result.comment4}
         *
         */
        transaction.commit();
        // transaction.rollback();
    }
}
