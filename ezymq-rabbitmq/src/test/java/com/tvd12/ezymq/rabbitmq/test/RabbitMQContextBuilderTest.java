package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcProducer;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQContextBuilderTest extends RabbitBaseTest {

    protected static Logger logger =
        LoggerFactory.getLogger(RabbitMQContextBuilderTest.class);

    public static void main(String[] args) throws Exception {
        int prefetchCount = 100;
        EzyRabbitMQProxy context = EzyRabbitMQProxy.builder()
            .connectionFactory(connectionFactory)
            .scan("com.tvd12.ezymq.rabbitmq.test.entity")
            .mapRequestType("fibonacci", int.class)
            .mapRequestType("test", String.class)
            .mapRequestType("", String.class)
            .settingsBuilder()
            .queueArgument("rmqia-rpc-queue", "x-max-length-bytes", 1024000)
            .topicSettingBuilder("test")
            .prefetchCount(prefetchCount)
            .exchange("rmqia-topic-exchange")
            .producerEnable(true)
            .clientRoutingKey("rmqia-topic-routing-key")
            .consumerEnable(true)
            .serverQueueName("mqia-topic")
            .parent()
            .rpcProducerSettingBuilder("fibonacci")
            .prefetchCount(prefetchCount)
            .defaultTimeout(300 * 1000)
            .exchange("rmqia-rpc-exchange")
            .requestQueueName("rmqia-rpc-queue")
            .requestRoutingKey("rmqia-rpc-routing-key")
            .replyQueueName("rmqia-rpc-client-queue")
            .replyRoutingKey("rmqia-rpc-client-routing-key")
            .parent()
            .rpcConsumerSettingBuilder("fibonacci")
            .prefetchCount(prefetchCount)
            .requestQueueName("rmqia-rpc-queue")
            .exchange("rmqia-rpc-exchange")
            .replyRoutingKey("rmqia-rpc-client-routing-key")
            .addRequestHandler(
                "fibonacci",
                new EzyRabbitRequestHandler<Integer>() {
                    @Override
                    public Object handle(Integer request) throws Exception {
                        return request + 3;
                    }
                }
            )
            .parent()
            .parent()
            .build();
        EzyRabbitTopic<String> topic = context.getTopic("test");
        topic.addConsumer(message -> logger.info("topic message: " + message));

        long startTopicTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            topic.publish("hello topic " + i);
        }
        long elapsedTopicTime = System.currentTimeMillis() - startTopicTime;
        System.out.println("elapsedTopicTime: " + elapsedTopicTime);

        EzyRabbitRpcProducer consumer = context.getRpcProducer("fibonacci");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
//			System.out.println("rabbit rpc start call: " + i);
            consumer.call("fibonacci", 100, int.class);
//			int result = consumer.call("fibonacci", 100, int.class);
//			System.out.println("i = " + i + ", result = " + result);
        }
        System.out.println("elapsed = " + (System.currentTimeMillis() - start));
        context.close();

        //noinspection InfiniteLoopStatement
        while (true) {
            //noinspection BusyWait
            Thread.sleep(100);
        }
    }

}
