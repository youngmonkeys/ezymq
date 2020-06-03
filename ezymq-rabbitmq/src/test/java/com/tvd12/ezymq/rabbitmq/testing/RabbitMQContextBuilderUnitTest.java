package com.tvd12.ezymq.rabbitmq.testing;

import org.testng.annotations.Test;

import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.exception.NotFoundException;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQContext;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcCaller;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitActionInterceptor;
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
				.settingsBuilder()
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
	
}
