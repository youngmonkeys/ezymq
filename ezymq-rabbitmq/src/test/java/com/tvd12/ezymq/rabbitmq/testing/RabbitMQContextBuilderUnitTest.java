package com.tvd12.ezymq.rabbitmq.testing;

import org.testng.annotations.Test;

import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleDeserializer;
import com.tvd12.ezyfox.codec.MsgPackSimpleSerializer;
import com.tvd12.ezyfox.collect.Sets;
import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezyfox.reflect.EzyReflectionProxy;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQContext;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcCaller;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitBytesDataCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitBytesEntityCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitActionInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitMessageConsumer;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;
import com.tvd12.ezymq.rabbitmq.testing.mockup.ConnectionFactoryMockup;

public class RabbitMQContextBuilderUnitTest {

	@Test
	public void test() throws Exception {
		ConnectionFactoryMockup connectionFactory = new ConnectionFactoryMockup();
		EzyRabbitMQContext context = EzyRabbitMQContext.builder()
				.connectionFactory(connectionFactory)
				.scan("com.tvd12.ezymq.rabbitmq.testing.entity")
				.mapRequestType("fibonaci", int.class)
				.mapRequestType("test", String.class)
				.mapRequestType("", String.class)
				.settingsBuilder()
				.topicSettingBuilder("test")
					.exchange("rmqia-topic-exchange")
					.clientEnable(true)
					.clientRoutingKey("rmqia-topic-routing-key")
					.serverEnable(true)
					.serverQueueName("mqia-topic")
					.parent()
				.rpcCallerSettingBuilder("fibonaci")
					.defaultTimeout(300 * 1000)
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
						int value = (int)a;
						if(value == 0)
							throw new NotFoundException("not found value 0");
						if(value == -1)
							throw new BadRequestException(1, "value = -1 invalid");
						if(value == -2)
							throw new IllegalArgumentException("value = -2 invalid");
						if(value == -3)
							throw new UnsupportedOperationException("value = -3 not accepted");
						if(value < -3)
							throw new IllegalStateException("server maintain");
						return value + 3;
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
		for(int i = 0 ; i < 1 ; ++i) {
			System.out.println("rabbit rpc start call: " + i);
			try {
				caller.fire("fibonaci", 50);
				caller.fire("fibonaci", 0);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			try {
				int result = caller.call("fibonaci", 100, int.class);
				System.out.println("i = " + i + ", result = " + result);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			try {
				caller.call("fibonaci", 0, int.class);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			try {
				caller.call("fibonaci", -1, int.class);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			try {
				caller.call("fibonaci", -2, int.class);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			try {
				caller.call("fibonaci", -3, int.class);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			try {
				caller.call("fibonaci", -4, int.class);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		Thread.sleep(100);
		System.out.println("elapsed = " + (System.currentTimeMillis() - start));
		context.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSetComponents() throws Exception {
		EzyRabbitSettings settings = EzyRabbitSettings.builder()
				.build();
		EzyBindingContext bindingContext = EzyBindingContext.builder()
				.scan("com.tvd12.ezymq.rabbitmq.testing.entity")
				.build();
		EzyMessageSerializer messageSerializer = newMessageSerializer();
		EzyMessageDeserializer messageDeserializer = newMessageDeserializer();
		EzyEntityCodec entityCodec = EzyRabbitBytesEntityCodec.builder()
				.marshaller(bindingContext.newMarshaller())
				.unmarshaller(bindingContext.newUnmarshaller())
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.build();
		EzyRabbitDataCodec dataCodec = EzyRabbitBytesDataCodec.builder()
				.marshaller(bindingContext.newMarshaller())
				.unmarshaller(bindingContext.newUnmarshaller())
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.mapRequestType("fibonaci", int.class)
				.mapRequestType("test", String.class)
				.build();
		EzyRabbitMQContext.builder()
				.scan("com.tvd12.ezymq.rabbitmq.testing.entity")
				.scan("com.tvd12.ezymq.rabbitmq.testing.entity", "com.tvd12.ezymq.rabbitmq.testing.entity")
				.scan(Sets.newHashSet("com.tvd12.ezymq.rabbitmq.testing.entity"))
				.scan(new EzyReflectionProxy("com.tvd12.ezymq.rabbitmq.testing.entity"))
				.settings(settings)
				.bindingContext(bindingContext)
				.marshaller(bindingContext.newMarshaller())
				.unmarshaller(bindingContext.newUnmarshaller())
				.messageSerializer(messageSerializer)
				.messageDeserializer(messageDeserializer)
				.entityCodec(entityCodec)
				.dataCodec(dataCodec)
				.mapRequestTypes(EzyMapBuilder.mapBuilder()
						.put("a", int.class)
						.build())
				.build();
	}
	
	protected static EzyMessageSerializer newMessageSerializer() {
		return new MsgPackSimpleSerializer();
	}
	
	protected static EzyMessageDeserializer newMessageDeserializer() {
		return new MsgPackSimpleDeserializer();
	}
	
}
