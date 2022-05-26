package com.tvd12.ezymq.kafka.testing;

import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.entity.EzyObject;
import com.tvd12.ezymq.kafka.testing.entity.KafkaChatMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.Collections;

public class KafkaExample1 {

    private final static String TOPIC = "my-example-topic";

    public static void main(String... args) {
//		runProducer(5);
        runConsumer();
    }

    private static EzyBindingContext newBindingContext() {
        return EzyBindingContext.builder()
            .scan("com.tvd12.ezymq.kafka.testing.entity")
            .build();
    }

    @SuppressWarnings("unchecked")
    private static Producer<Long, EzyObject> createProducer() {
        Producer<Long, EzyObject> producer = TestUtil.newProducer();
        return producer;
    }

    @SuppressWarnings("unchecked")
    private static Consumer<Long, EzyObject> createConsumer() {
        Consumer<Long, EzyObject> consumer = TestUtil.newConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Long, EzyObject> producer = createProducer();
        long time = System.currentTimeMillis();

        EzyBindingContext bindingContext = newBindingContext();
        EzyMarshaller marshaller = bindingContext.newMarshaller();

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                KafkaChatMessage message = new KafkaChatMessage(index, "message#" + index);
                EzyObject value = marshaller.marshal(message);
                final ProducerRecord<Long, EzyObject> record = new ProducerRecord<>(TOPIC, index, value);

                RecordMetadata metadata = producer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    static void runConsumer() {
        Consumer<Long, EzyObject> consumer = createConsumer();

        while (true) {
            final ConsumerRecords<Long, EzyObject> consumerRecords = consumer.poll(Duration.ofMillis(100));

            if (consumerRecords.count() == -1) {
                break;
            }

            consumerRecords.forEach(record -> {
                System.out.println(
                    "Got Record: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
}
