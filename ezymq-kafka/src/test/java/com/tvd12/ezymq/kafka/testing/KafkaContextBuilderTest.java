package com.tvd12.ezymq.kafka.testing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.EzyKafkaProxy;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;

public class KafkaContextBuilderTest extends KafkaBaseTest {

	public void test() throws Exception {
		EzyKafkaProxy kafkaContext = EzyKafkaProxy.builder()
				.scan("com.tvd12.ezymq.kafka.testing.entity")
				.mapMessageType("hello", String.class)
				.settingsBuilder()
					.property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
					.producerSettingBuilder("clientA")
						.topic(TOPIC)
						.property(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
					.parent()
					.consumerSettingBuilder("serverA")
						.topic(TOPIC)
						.property(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer")
						.addMessageHandler("hello", new EzyKafkaMessageHandler<String>() {
							@Override
							public void process(String request) throws Exception {
								System.out.println("hello: " + request);
							}
						})
						.messageInterceptor(new EzyKafkaMessageInterceptor() {
							
							@Override
							public void postHandle(String topic, String cmd, Object requestData, Throwable e) {
								e.printStackTrace();
							}
							
							@Override
							public void postHandle(String topic, String cmd, Object requestData, Object responseData) {
								System.out.println(Thread.currentThread() + ": response: " + cmd);
								
							}
							
							@Override
							public void preHandle(String topic, String cmd, Object requestData) {
								System.out.println(Thread.currentThread() + ": request: " + cmd);
							}
						})
					.parent()
				.parent()
				.build();
		EzyKafkaProducer caller = kafkaContext.getProducer("clientA");
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
