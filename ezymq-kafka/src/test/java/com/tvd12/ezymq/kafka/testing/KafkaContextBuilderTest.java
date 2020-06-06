package com.tvd12.ezymq.kafka.testing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.tvd12.ezymq.kafka.EzyKafkaCaller;
import com.tvd12.ezymq.kafka.EzyKafkaContext;
import com.tvd12.ezymq.kafka.handler.EzyKafkaActionInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRequestHandler;

public class KafkaContextBuilderTest extends KafkaBaseTest {

	public void test() throws Exception {
		EzyKafkaContext kafkaContext = EzyKafkaContext.builder()
				.scan("com.tvd12.ezymq.kafka.testing.entity")
				.mapRequestType("hello", String.class)
				.settingsBuilder()
					.property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
					.callerSettingBuilder("clientA")
						.topic(TOPIC)
						.property(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
					.parent()
					.handlerSettingBuilder("serverA")
						.topic(TOPIC)
						.property(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer")
						.addRequestHandler("hello", new EzyKafkaRequestHandler<String>() {
							@Override
							public void process(String request) throws Exception {
								System.out.println("hello: " + request);
							}
						})
						.actionInterceptor(new EzyKafkaActionInterceptor() {
							
							@Override
							public void intercept(String cmd, Object requestData, Exception e) {
								e.printStackTrace();
							}
							
							@Override
							public void intercept(String cmd, Object requestData, Object responseData) {
								System.out.println(Thread.currentThread() + ": response: " + cmd);
								
							}
							
							@Override
							public void intercept(String cmd, Object requestData) {
								System.out.println(Thread.currentThread() + ": request: " + cmd);
							}
						})
					.parent()
				.parent()
				.build();
		EzyKafkaCaller caller = kafkaContext.getCaller("clientA");
		caller.send("hello", "world");
		Thread.sleep(1000);
		kafkaContext.close();
	}
	
	public static void main(String[] args) throws Exception {
		new KafkaContextBuilderTest().test();
		while(true) {
			Thread.sleep(1000);
		}
	}
	
}
