package com.tvd12.ezymq.rabbitmq.testing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tvd12.ezymq.rabbitmq.EzyRabbitMQContext;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcCaller;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitActionInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageConsumer;

public class RabbitMQContextBuilderTest extends RabbitBaseTest {

	protected static Logger logger = 
			LoggerFactory.getLogger(RabbitMQContextBuilderTest.class);
	
	public static void main(String[] args) throws Exception {
		int prefetchCount = 100;
		EzyRabbitMQContext context = EzyRabbitMQContext.builder()
				.connectionFactory(connectionFactory)
				.scan("com.tvd12.ezymq.rabbitmq.testing.entity")
				.mapRequestType("fibonaci", int.class)
				.mapRequestType("test", String.class)
				.mapRequestType("", String.class)
				.settingsBuilder()
				.queueArgument("rmqia-rpc-queue", "x-max-length-bytes", 1024000)
				.topicSettingBuilder("test")
					.prefetchCount(prefetchCount)
					.exchange("rmqia-topic-exchange")
					.clientEnable(true)
					.clientRoutingKey("rmqia-topic-routing-key")
					.serverEnable(true)
					.serverQueueName("mqia-topic")
					.parent()
				.rpcCallerSettingBuilder("fibonaci")
					.prefetchCount(prefetchCount)
					.defaultTimeout(300 * 1000)
					.exchange("rmqia-rpc-exchange")
					.requestQueueName("rmqia-rpc-queue")
					.requestRoutingKey("rmqia-rpc-routing-key")
					.replyQueueName("rmqia-rpc-client-queue")
					.replyRoutingKey("rmqia-rpc-client-routing-key")
					.parent()
				.rpcHandlerSettingBuilder("fibonaci")
					.prefetchCount(prefetchCount)
					.requestQueueName("rmqia-rpc-queue")
					.exchange("rmqia-rpc-exchange")
					.replyRoutingKey("rmqia-rpc-client-routing-key")
					.addRequestHandler("fibonaci", a -> {
						return (int)a + 3;
					})
					.actionInterceptor(new EzyRabbitActionInterceptor() {
						
						@Override
						public void intercept(String cmd, Object requestData, Exception e) {
							e.printStackTrace();
						}
						
						@Override
						public void intercept(String cmd, Object requestData, Object responseData) {
							
						}
						
						@Override
						public void intercept(String cmd, Object requestData) {
							
						}
					})
					.parent()
				.parent()
				.build();
		EzyRabbitTopic<String> topic = context.getTopic("test");
		topic.addConsumer(new EzyRabbitMessageConsumer<String>() {
			
			@Override
			public void consume(String message) {
				logger.info("topic message: " + message);
			}
		});
		long startTopicTime = System.currentTimeMillis();
		for(int i = 0 ; i < 1000 ; ++i) {
			topic.publish("hello topic " + i);
		}
		long elapsedTopicTime = System.currentTimeMillis() - startTopicTime;
		System.out.println("elapsedTopicTime: " + elapsedTopicTime);
		
		EzyRabbitRpcCaller caller = context.getRpcCaller("fibonaci");
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 1000 ; ++i) {
//			System.out.println("rabbit rpc start call: " + i);
			caller.call("fibonaci", 100, int.class);
//			int result = caller.call("fibonaci", 100, int.class);
//			System.out.println("i = " + i + ", result = " + result);
		}
		System.out.println("elapsed = " + (System.currentTimeMillis() - start));
		context.close();
	}
	
}
