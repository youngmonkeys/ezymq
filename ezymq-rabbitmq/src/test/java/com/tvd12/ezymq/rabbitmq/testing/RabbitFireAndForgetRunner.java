package com.tvd12.ezymq.rabbitmq.testing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.binding.impl.EzySimpleBindingContext;
import com.tvd12.ezyfox.builder.EzyArrayBuilder;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleDeserializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleSerializer;
import com.tvd12.ezyfox.factory.EzyEntityFactory;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcCaller;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcHandler;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitBytesDataCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitBytesEntityCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactoryBuilder;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitActionLogInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;

public class RabbitFireAndForgetRunner extends EzyLoggable {

	private EzyMarshaller marshaller;
	private EzyUnmarshaller unmarshaller;
	private EzyMessageSerializer messageSerializer;
	private EzyMessageDeserializer messageDeserializer;

	private EzyBindingContext bindingContext;
	private EzyEntityCodec entityCodec;
	private EzyRabbitDataCodec dataCodec;
	private EzyRabbitRequestHandlers requestHandlers;
	
	public RabbitFireAndForgetRunner() {
		this.messageSerializer = newMessageSerializer();
		this.messageDeserializer = newMessageDeserializer();
		this.bindingContext = newBindingContext();
		this.marshaller = bindingContext.newMarshaller();
		this.unmarshaller = bindingContext.newUnmarshaller();
		this.entityCodec = EzyRabbitBytesEntityCodec.builder()
				.marshaller(marshaller)
				.unmarshaller(unmarshaller)
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.build();
		this.dataCodec = EzyRabbitBytesDataCodec.builder()
				.marshaller(marshaller)
				.unmarshaller(unmarshaller)
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.mapRequestType("fibonaci", int.class)
				.build();
		this.requestHandlers = new EzyRabbitRequestHandlers();
		this.requestHandlers.addHandler("fibonaci", a -> {
			return (int)a + 3;
		});
	}
	
	public static void main(String[] args) throws Exception {
		EzyEntityFactory.create(EzyArrayBuilder.class);
		RabbitFireAndForgetRunner runner = new RabbitFireAndForgetRunner();
		runner.startServer();
		runner.sleep();
//		runner.asynRpc();
		runner.fire();
	}
	
	protected void startServer() throws Exception {
		new Thread(()-> {
			try {
				System.out.println("thread-" + Thread.currentThread().getName() + ": start server");
				EzyRabbitRpcServer server = newServer();
				EzyRabbitRpcHandler handler = EzyRabbitRpcHandler.builder()
						.server(server)
						.dataCodec(dataCodec)
						.requestHandlers(requestHandlers)
						.actionInterceptor(new EzyRabbitActionLogInterceptor())
						.build();
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
					fire();
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
		}
		.start();
	}
	
	protected void fire() throws Exception {
		EzyRabbitRpcClient client = newClient();
		EzyRabbitRpcCaller caller = EzyRabbitRpcCaller.builder()
				.client(client)
				.entityCodec(entityCodec)
				.build();
		System.out.println("thread-" + Thread.currentThread().getName() + ": start rpc");
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 1000 ; ++i) {
			caller.fire("fibonaci", 100);
		}
		System.out.println("elapsed = " + (System.currentTimeMillis() - start));
	}
	
	protected EzyRabbitRpcClient newClient() throws Exception {
		ConnectionFactory connectionFactory = new EzyRabbitConnectionFactoryBuilder()
			.build();
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		channel.basicQos(1);
		channel.exchangeDeclare("rmqia-rpc-exchange", "direct");
		channel.queueDeclare("rmqia-rpc-queue", false, false, false, null);
		channel.queueDeclare("rmqia-rpc-client-queue", false, false, false, null);
		channel.queueBind("rmqia-rpc-queue", "rmqia-rpc-exchange", "rmqia-rpc-routing-key");
		channel.queueBind("rmqia-rpc-client-queue", "rmqia-rpc-exchange", "rmqia-rpc-client-routing-key");
		return EzyRabbitRpcClient.builder()
				.defaultTimeout(300 * 1000)
				.channel(channel)
				.exchange("rmqia-rpc-exchange")
				.routingKey("rmqia-rpc-routing-key")
				.replyQueueName("rmqia-rpc-client-queue")
				.build();
	}
	
	protected EzyRabbitRpcServer newServer() throws Exception {
		ConnectionFactory connectionFactory = new EzyRabbitConnectionFactoryBuilder()
				.build();
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		channel.basicQos(1);
		channel.exchangeDeclare("rmqia-rpc-exchange", "direct");
		channel.queueDeclare("rmqia-rpc-queue", false, false, false, null);
		channel.queueDeclare("rmqia-rpc-client-queue", false, false, false, null);
		channel.queueBind("rmqia-rpc-queue", "rmqia-rpc-exchange", "rmqia-rpc-routing-key");
		channel.queueBind("rmqia-rpc-client-queue", "rmqia-rpc-exchange", "rmqia-rpc-client-routing-key");
		return EzyRabbitRpcServer.builder()
				.queueName("rmqia-rpc-queue")
				.exchange("rmqia-rpc-exchange")
				.replyRoutingKey("rmqia-rpc-client-routing-key")
				.channel(channel)
				.build();
	}
	
	protected EzyMessageSerializer newMessageSerializer() {
		return new MsgPackSimpleSerializer();
	}
	
	protected EzyMessageDeserializer newMessageDeserializer() {
		return new MsgPackSimpleDeserializer();
	}
	
	private EzyBindingContext newBindingContext() {
		return EzySimpleBindingContext.builder()
				.scan("com.tvd12.ezymq.rabbitmq.testing.entity")
				.build();
	}
	
}
