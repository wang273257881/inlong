package inlong.tubemq.ctl;

import java.util.List;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.inlong.tubemq.client.config.ConsumerConfig;
import org.apache.inlong.tubemq.client.config.TubeClientConfig;
import org.apache.inlong.tubemq.client.consumer.ConsumePosition;
import org.apache.inlong.tubemq.client.consumer.ConsumerResult;
import org.apache.inlong.tubemq.client.consumer.PullMessageConsumer;
import org.apache.inlong.tubemq.client.factory.MessageSessionFactory;
import org.apache.inlong.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.inlong.tubemq.client.producer.MessageProducer;
import org.apache.inlong.tubemq.client.producer.MessageSentCallback;
import org.apache.inlong.tubemq.client.producer.MessageSentResult;
import org.apache.inlong.tubemq.corebase.Message;
import org.apache.inlong.tubemq.corebase.utils.ThreadUtils;

public class MsgManager {
    public static void produceMsg(String masterHostAndPort, String topic, String msg) throws Throwable {
        final TubeClientConfig clientConfig = new TubeClientConfig(masterHostAndPort);
        final MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
        final MessageProducer messageProducer = messageSessionFactory.createProducer();
        byte[] bodyData = StringUtils.getBytesUtf8(msg);
        messageProducer.publish(topic);
        final Message message = new Message(topic, bodyData);
        messageProducer.sendMessage(message, new MessageSentCallback(){
            @Override
            public void onMessageSent(MessageSentResult result) {
                if (result.isSuccess()) {
                    System.out.println("async send message : " + msg);
                } else {
                    System.out.println("async send message failed : " + result.getErrMsg());
                }
            }
            @Override
            public void onException(Throwable e) {
                System.out.println("async send message error : " + e);
            }
        });
        messageProducer.shutdown();
    }

    public static void consumeMsg(String masterHostAndPort, String topic, String consumeGroup) throws Throwable {
        final ConsumerConfig consumerConfig = new ConsumerConfig(masterHostAndPort, consumeGroup);
        consumerConfig.setConsumePosition(ConsumePosition.CONSUMER_FROM_LATEST_OFFSET);
        final MessageSessionFactory messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        final PullMessageConsumer messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
        messagePullConsumer.subscribe(topic, null);
        messagePullConsumer.completeSubscribe();
        // wait for client to join the exact consumer queue that consumer group allocated
        while (!messagePullConsumer.isPartitionsReady(1000)) {
            ThreadUtils.sleep(1000);
        }
        System.out.println(String.format("consumer %s started at %s on %s", consumeGroup, masterHostAndPort, topic));
        while (true) {
            ConsumerResult result = messagePullConsumer.getMessage();
            if (result.isSuccess()) {
                List<Message> messageList = result.getMessageList();
                for (Message message : messageList) {
                    System.out.println("received message : " + new String(message.getData()));
                }
                messagePullConsumer.confirmConsume(result.getConfirmContext(), true);
            }
        }
    }
}
