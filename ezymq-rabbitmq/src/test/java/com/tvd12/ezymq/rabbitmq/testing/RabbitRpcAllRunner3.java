package com.tvd12.ezymq.rabbitmq.testing;

import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.tvd12.ezyfox.builder.EzyArrayBuilder;
import com.tvd12.ezyfox.factory.EzyEntityFactory;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcCaller;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcHandler;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitResponseConsumer;

public class RabbitRpcAllRunner3 extends RabbitBaseTest {

	private EzyRabbitRequestHandlers requestHandlers;
	
	public RabbitRpcAllRunner3() {
		this.requestHandlers = new EzyRabbitRequestHandlers();
		this.requestHandlers.addHandler("fibonaci", a -> {
			return (int)a + 3;
		});
	}
	
	public static void main(String[] args) throws Exception {
		EzyEntityFactory.create(EzyArrayBuilder.class);
		RabbitRpcAllRunner3 runner = new RabbitRpcAllRunner3();
		runner.startServer();
		runner.sleep();
//		runner.asynRpc();
		runner.rpc();
	}
	
	@SuppressWarnings("resource")
	protected void startServer() throws Exception {
		new Thread(()-> {
			try {
				System.out.println("thread-" + Thread.currentThread().getName() + ": start server");
				EzyRabbitRpcServer server = newServer();
				EzyRabbitRpcHandler handler = new EzyRabbitRpcHandler(server, dataCodec, requestHandlers);
				handler.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
		
	}
	
	protected void sleep() throws Exception {
		Thread.sleep(1000);
	}
	
	protected void asynRpc() {
		new Thread() {
			public void run() {
				try {
					rpc();
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
		}
		.start();
	}
	
	@SuppressWarnings("resource")
	protected void rpc() throws Exception {
		EzyRabbitRpcClient client = newClient();
		EzyRabbitRpcCaller caller = new EzyRabbitRpcCaller(client, entityCodec);
		System.out.println("thread-" + Thread.currentThread().getName() + ": start rpc");
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 1000 ; ++i) {
			System.out.println("rabbit rpc start call: " + i);
			int result = caller.call("fibonaci", 100, int.class);
			System.out.println("i = " + i + ", result = " + result);
		}
		System.out.println("elapsed = " + (System.currentTimeMillis() - start));
	}
	
	protected EzyRabbitRpcClient newClient() throws Exception {
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		channel.basicQos(1);
		channel.exchangeDeclare("rmqia-rpc-exchange", "direct");
		try {
			Map<String, Object> props = new HashMap<>();
			props.put("x-max-length-bytes", 1024000);
			channel.queueDeclare("rmqia-rpc-queue", false, false, false, props);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		channel.queueDeclare("rmqia-rpc-client-queue", false, false, false, null);
		channel.queueDeclare("rmqia-rpc-client-queue-private", false, false, false, null);
		channel.queueDeclare("rmqia-rpc-client-queue-backup", false, false, false, null);
		channel.queueBind("rmqia-rpc-queue", "rmqia-rpc-exchange", "rmqia-rpc-routing-key");
		channel.queueBind("rmqia-rpc-client-queue", "rmqia-rpc-exchange", "rmqia-rpc-client-routing-key");
		channel.queueBind("rmqia-rpc-client-queue-private", "rmqia-rpc-exchange", "rmqia-rpc-client-routing-key-private");
		channel.queueBind("rmqia-rpc-client-queue-backup", "rmqia-rpc-exchange", "rmqia-rpc-client-routing-key-backup");
		return EzyRabbitRpcClient.builder()
				.defaultTimeout(300 * 1000)
				.channel(channel)
				.exchange("rmqia-rpc-exchange")
				.routingKey("rmqia-rpc-routing-key")
				.replyQueueName("rmqia-rpc-client-queue-private")
				.replyRoutingKey("rmqia-rpc-client-routing-key-private")
				.unconsumedResponseConsumer(new EzyRabbitResponseConsumer() {
					@Override
					public void consume(BasicProperties properties, byte[] responseBody) {
						try {
							int answer = entityCodec.deserialize(responseBody, int.class);
							System.out.println("unconsumed fibonaci response: " + answer);
						}
						catch(Exception e) {
							e.printStackTrace();
						}
					}
				})
				.build();
	}
	
	protected EzyRabbitRpcServer newServer() throws Exception {
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		channel.basicQos(1);
		channel.exchangeDeclare("rmqia-rpc-exchange", "direct");
		try {
			Map<String, Object> props = new HashMap<>();
			props.put("x-max-length-bytes", 1024000);
			channel.queueDeclare("rmqia-rpc-queue", false, false, false, props);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		channel.queueDeclare("rmqia-rpc-client-queue", false, false, false, null);
		channel.queueDeclare("rmqia-rpc-client-queue-private", false, false, false, null);
		channel.queueBind("rmqia-rpc-queue", "rmqia-rpc-exchange", "rmqia-rpc-routing-key");
		channel.queueBind("rmqia-rpc-client-queue", "rmqia-rpc-exchange", "rmqia-rpc-client-routing-key");
		channel.queueBind("rmqia-rpc-client-queue-private", "rmqia-rpc-exchange", "rmqia-rpc-client-routing-key-private");
		return EzyRabbitRpcServer.builder()
				.queueName("rmqia-rpc-queue")
				.exchange("rmqia-rpc-exchange")
				.replyRoutingKey("rmqia-rpc-client-routing-key")
				.channel(channel)
				.build();
	}
	
}
