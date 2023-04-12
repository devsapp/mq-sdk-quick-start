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
    private static readonly string TAG = "${TAG}";
    private static readonly string KEY = "${KEY}";
    private static readonly string BODY = "${BODY}";
    private static readonly string MESSAGE_GROUP = "${MESSAGE_GROUP}";

    static async Task Main()
    {
        var configBuilder = new ClientConfig.Builder().SetEndpoints(ENDPOINT);

        /**
        * ${quickstart.ak.comment1}
        * ${quickstart.ak.comment2}
        */
        // configBuilder.SetCredentialsProvider(new StaticSessionCredentialsProvider("Instance UserName", "Instance Password"));
        var clientConfig = configBuilder.Build();

        /**
        * ${quickstart.provider.comment1}
        * ${quickstart.provider.comment2}
        * ${quickstart.provider.comment3}
        */
        var producer = await new Producer.Builder()
            .SetClientConfig(clientConfig)
            .SetTopics(TOPIC_NAME)
            .Build();

        var builder = new Message.Builder()
            // ${quickstart.setTopic.comment}
            .SetTopic(TOPIC_NAME)
            // ${quickstart.body.comment}
            .SetBody(Encoding.UTF8.GetBytes(BODY));

        if (!string.IsNullOrEmpty(KEY))
        {
            // ${quickstart.key.comment}
            builder.SetKeys(KEY);
        }

        if (!string.IsNullOrEmpty(TAG))
        {
            // ${quickstart.tag.comment}
            builder.SetTag(TAG);
        }

        if (!string.IsNullOrEmpty(MESSAGE_GROUP))
        {
            // ${quickstart.messageGroup.comment}
            builder.SetMessageGroup(MESSAGE_GROUP);
        }

        // ${quickstart.property.comment}
        ${ quickstart.property.content}

        // ${quickstart.sendMessage.comment}
        var sendReceipt = await producer.Send(builder.Build());
        Console.WriteLine("Send message successfully, messageId=" + sendReceipt.MessageId);

        // ${quickstart.producer.close.comment}
        await producer.DisposeAsync();
    }
}
