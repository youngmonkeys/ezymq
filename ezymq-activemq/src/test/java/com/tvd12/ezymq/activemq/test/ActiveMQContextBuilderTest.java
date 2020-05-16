package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQContext;
import com.tvd12.ezymq.activemq.EzyActiveRpcCaller;
import com.tvd12.ezymq.activemq.handler.EzyActiveActionInterceptor;

public class ActiveMQContextBuilderTest extends ActiveMQBaseTest {

	public static void main(String[] args) {
		EzyActiveMQContext context = EzyActiveMQContext.builder()
				.scan("com.tvd12.ezymq.activemq.test.entity")
				.mapRequestType("fibonaci", int.class)
				.mapRequestType("test", String.class)
				.settingsBuilder()
				.rpcCallerSettingBuilder("fibonaci")
					.defaultTimeout(300 * 1000)
					.requestQueueName("rpc-request-test-1")
					.replyQueueName("rpc-response-test-1")
					.parent()
				.rpcHandlerSettingBuilder("fibonaci")
					.requestQueueName("rpc-request-test-1")
					.replyQueueName("rpc-response-test-1")
					.addRequestHandler("fibonaci", a -> {
						return (int)a + 3;
					})
					.actionInterceptor(new EzyActiveActionInterceptor() {
						
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
		EzyActiveRpcCaller caller = context.getRpcCaller("fibonaci");
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 1000 ; ++i) {
			System.out.println("rabbit rpc start call: " + i);
			int result = caller.call("fibonaci", 100, int.class);
			System.out.println("i = " + i + ", result = " + result);
		}
		System.out.println("elapsed = " + (System.currentTimeMillis() - start));
	}
	
}
