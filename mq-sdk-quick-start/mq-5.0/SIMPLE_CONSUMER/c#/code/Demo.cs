using System;
using System.Text;
using Org.Apache.Rocketmq;

class Demo
{
    /**
    * ${quickstart.endpoint.comment1}
    * ${quickstart.endpoint.comment2}
    * ${quickstart.endpoint.comment3}
    */
    private static readonly string ENDPOINT = "${ENDPOINT}";
    private static readonly string TOPIC_NAME = "${TOPIC_NAME}";
    private static readonly string FILTER_EXPRESSION = "${FILTER_EXPRESSION}";
    private static readonly string CONSUMER_GROUP_ID = "${CONSUMER_GROUP_ID}";
    private static readonly long AWAIT_DURATION_SECONDS = 10;
    private static readonly int MAX_MESSAGE = ${MAX_MESSAGE_NUM};
    private static readonly long INVISIBLE_DURATION_SECONDS = ${INVISIBLE_DURATION_SECONDS};

    static async Task Main()
    {
        var configBuilder = new ClientConfig.Builder().SetEndpoints(ENDPOINT);

        /**
        * ${quickstart.ak.comment1}
        * ${quickstart.ak.comment2}
        */
        // configBuilder.SetCredentialsProvider(new StaticSessionCredentialsProvider("Instance UserName", "Instance Password"));
        var clientConfig = configBuilder.Build();

        var simpleConsumer = await new SimpleConsumer.Builder()
            .SetClientConfig(clientConfig)
            // ${quickstart.consumer.group.comment}
            .SetConsumerGroup(CONSUMER_GROUP_ID)
            // ${quickstart.consumer.topic.comment}
            .SetSubscriptionExpression(new Dictionary<string, FilterExpression>
             { { TOPIC_NAME, new FilterExpression(FILTER_EXPRESSION) } })
            // ${quickstart.consumer.awaitDuration.comment}
            .SetAwaitDuration(TimeSpan.FromSeconds(AWAIT_DURATION_SECONDS))
            .Build();

        while (true)
        {
            var messageViews = await simpleConsumer.Receive(MAX_MESSAGE, TimeSpan.FromSeconds(INVISIBLE_DURATION_SECONDS));
            foreach (var message in messageViews)
            {
                // ${quickstart.consumer.ack.comment}
                await simpleConsumer.Ack(message);
                Console.WriteLine("Message is acknowledged successfully, messageId=" + message.MessageId);
            }
        }
        // ${quickstart.consumer.simple.close.comment}
        // await simpleConsumer.DisposeAsync();
    }
}
