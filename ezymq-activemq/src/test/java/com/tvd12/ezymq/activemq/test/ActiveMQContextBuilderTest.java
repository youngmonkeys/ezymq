package com.tvd12.ezymq.activemq.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tvd12.ezymq.activemq.EzyActiveMQContext;
import com.tvd12.ezymq.activemq.EzyActiveRpcCaller;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.handler.EzyActiveActionInterceptor;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageConsumer;

public class ActiveMQContextBuilderTest extends ActiveMQBaseTest {

	protected static Logger logger = 
			LoggerFactory.getLogger(ActiveMQContextBuilderTest.class);
	
	public static void main(String[] args) throws Exception {
		EzyActiveMQContext context = EzyActiveMQContext.builder()
				.scan("com.tvd12.ezymq.activemq.test.entity")
				.mapRequestType("fibonaci", int.class)
				.mapRequestType("test", String.class)
				.mapRequestType("", String.class)
				.settingsBuilder()
				.topicSettingBuilder("test")
					.clientEnable(true)
					.topicName("topic-test")
					.serverEnable(true)
					.serverThreadPoolSize(3)
					.parent()
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
		EzyActiveTopic<String> topic = context.getTopic("test");
		topic.addConsumer(new EzyActiveMessageConsumer<String>() {
			
			@Override
			public void consume(String message) {
				logger.info("topic message: " + message);
			}
		});
		long startTopicTime = System.currentTimeMillis();
		for(int i = 0 ; i < 100 ; ++i) {
			topic.publish("hello topic " + i);
		}
		long elapsedTopicTime = System.currentTimeMillis() - startTopicTime;
		System.out.println("elapsedTopicTime: " + elapsedTopicTime);
		EzyActiveRpcCaller caller = context.getRpcCaller("fibonaci");
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 1000 ; ++i) {
//			System.out.println("rabbit rpc start call: " + i);
			caller.call("fibonaci", 100, int.class);
//			int result = caller.call("fibonaci", 100, int.class);
//			System.out.println("i = " + i + ", result = " + result);
		}
		System.out.println("elapsed = " + (System.currentTimeMillis() - start));
		context.close();
		while(true) {
			Thread.sleep(100);
		}
	}
	
}
