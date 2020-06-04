package com.tvd12.ezymq.rabbitmq.testing;

import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcCaller;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitActionInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageConsumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQContext;

public class RabbitMQContextBuilderTest extends RabbitBaseTest {

	public static void main(String[] args) throws Exception {
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		channel.basicQos(1);
		channel.exchangeDeclare("rmqia-rpc-exchange", "direct");
		EzyRabbitMQContext context = EzyRabbitMQContext.builder()
//				.connectionFactory(connectionFactory)
				.scan("com.tvd12.ezymq.rabbitmq.testing.entity")
				.mapRequestType("fibonaci", int.class)
				.mapRequestType("test", String.class)
				.mapRequestType("", String.class)
				.settingsBuilder()
				.queueArgument("rmqia-rpc-queue", "x-max-length-bytes", 1024000)
				.topicSettingBuilder("test")
					.exchange("rmqia-topic-exchange")
					.clientEnable(true)
					.clientRoutingKey("rmqia-topic-routing-key")
					.serverEnable(true)
					.serverQueueName("mqia-topic")
					.parent()
				.rpcCallerSettingBuilder("fibonaci")
					.defaultTimeout(300 * 1000)
					.channel(channel)
					.exchange("rmqia-rpc-exchange")
					.requestQueueName("rmqia-rpc-queue")
					.requestRoutingKey("rmqia-rpc-routing-key")
					.replyQueueName("rmqia-rpc-client-queue")
					.replyRoutingKey("rmqia-rpc-client-routing-key")
					.parent()
				.rpcHandlerSettingBuilder("fibonaci")
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
				System.out.println("topic message: " + message);
			}
		});
		topic.publish("hello topic");
		EzyRabbitRpcCaller caller = context.getRpcCaller("fibonaci");
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 1000 ; ++i) {
			System.out.println("rabbit rpc start call: " + i);
			int result = caller.call("fibonaci", 100, int.class);
			System.out.println("i = " + i + ", result = " + result);
		}
		System.out.println("elapsed = " + (System.currentTimeMillis() - start));
	}
	
}
