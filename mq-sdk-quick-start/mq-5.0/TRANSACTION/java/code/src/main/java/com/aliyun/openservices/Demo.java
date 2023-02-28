package com.aliyun.openservices;

import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.ClientServiceProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;

public class Demo {
    // Please replace the ACCESS_KEY and SECRET_KEY with your RocketMQ instance
    // username and password.
    public static final String ACCESS_KEY = "";
    public static final String SECRET_KEY = "";

    // Please enable the public endpoint in instance detail page and replace the
    // following ENDPOINT parameter if you want to access it via internet.
    public static final String ENDPOINT = "${ENDPOINT}";

    public static final String TOPIC_NAME = "${TOPIC_NAME}";
    public static final String TAG = "${TAG}";
    public static final String KEY = "${KEY}";
    public static final String BODY = "${BODY}";

    public static void main(String[] args) throws ClientException, IOException {
        String accessKey = ACCESS_KEY;
        String secretKey = SECRET_KEY;
        if (accessKey.isEmpty() && System.getenv("USERNAME") != null) {
            accessKey = System.getenv("USERNAME");
        }
        if (secretKey.isEmpty() && System.getenv("PASSWORD") != null) {
            secretKey = System.getenv("PASSWORD");
        }

        TransactionChecker checker = messageView -> {
            System.out.println("Receive transactional message check, message=" + messageView.toString());

            // Return the transaction resolution according to your business logic.
            return TransactionResolution.COMMIT;
        };

        ClientServiceProvider provider = ClientServiceProvider.loadService();

        StaticSessionCredentialsProvider credentialsProvider = new StaticSessionCredentialsProvider(
                accessKey, secretKey);

        // In most case, you don't need to create too many producers, singleton pattern
        // is recommended.
        Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(
                        ClientConfiguration.newBuilder()
                                .setEndpoints(ENDPOINT)
                                .setCredentialProvider(credentialsProvider)
                                .build())
                // Set the topic name(s), which is optional but recommended. It makes producer
                // could prefetch the topic
                // route before message publishing.
                .setTopics(TOPIC_NAME)
                // Set transactional checker.
                .setTransactionChecker(checker)
                // May throw {@link ClientException} if the producer is not initialized.
                .build();

        final Transaction transaction = producer.beginTransaction();

        MessageBuilder builder = provider.newMessageBuilder()
                // Set topic for the current message.
                .setTopic(TOPIC_NAME)
                // Message body.
                .setBody(BODY.getBytes(StandardCharsets.UTF_8));

        if (!TAG.isEmpty()) {
            // Message secondary classifier of message besides topic.
            builder.setTag(TAG);
        }

        if (!KEY.isEmpty()) {
            // Key(s) of the message, another way to mark message besides message id.
            builder.setKeys(KEY);
        }

        try {
            final SendReceipt sendReceipt = producer.send(builder.build(), transaction);
            System.out.println("Send mq message success! Topic is:" + TOPIC_NAME + " msgId is: "
                    + sendReceipt.getMessageId().toString());
        } catch (Throwable t) {
            System.out.println(" Send mq message failed! Topic is:" + TOPIC_NAME);
            t.printStackTrace();
        }

        // Commit the transaction.
        transaction.commit();
        // Or rollback the transaction.
        // transaction.rollback();

        // Close the producer when you don't need it anymore.
        producer.close();
    }
}
