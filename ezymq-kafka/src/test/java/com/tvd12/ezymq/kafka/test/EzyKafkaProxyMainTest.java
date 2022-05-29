package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.EzyKafkaProxy;
import com.tvd12.ezymq.kafka.test.request.SumRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class EzyKafkaProxyMainTest extends KafkaBaseTest {

    public static void main(String[] args) throws Exception {
        EzyKafkaProxy kafkaContext = EzyKafkaProxy.builder()
            .scan("com.tvd12.ezymq.kafka.test")
            .settingsBuilder()
            .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
            .producerSettingBuilder("test")
            .topic("test")
            .property(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
            .parent()
            .consumerSettingBuilder("test")
            .topic("test")
            .property(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer")
            .parent()
            .parent()
            .build();
        EzyKafkaProducer consumer = kafkaContext.getProducer("test");
        consumer.send("sum", new SumRequest(1, 2));
        Thread.sleep(1000);
        kafkaContext.close();
    }
}
