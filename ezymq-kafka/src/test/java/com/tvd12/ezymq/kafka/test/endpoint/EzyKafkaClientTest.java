package com.tvd12.ezymq.kafka.test.endpoint;

import com.tvd12.ezymq.kafka.endpoint.EzyKafkaClient;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyKafkaClientTest extends BaseTest  {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void test() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        Producer producer = mock(Producer.class);
        EzyKafkaClient sut = EzyKafkaClient.builder()
            .topic(topic)
            .producer(producer)
            .property("hello", "world")
            .build();

        // when
        String cmd = RandomUtil.randomShortAlphabetString();
        byte[] message = RandomUtil.randomShortByteArray();
        sut.send(cmd, message);

        // then
        verify(producer, times(1)).send(
            new ProducerRecord<>(topic, cmd, message)
        );
        sut.close();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testWithTopicIsNull() {
        // given
        Serializer serializer = mock(Serializer.class);
        Producer producerTest = mock(Producer.class);
        EzyKafkaClient sut = new EzyKafkaClient.Builder() {
            @Override
            protected Producer newProducer(Serializer serializer) {
                return producerTest;
            }
        }
        .serializer(serializer)
        .build();

        // when
        String cmd = RandomUtil.randomShortAlphabetString();
        byte[] message = RandomUtil.randomShortByteArray();
        sut.send(cmd, message);

        // then
        verify(producerTest, times(1)).send(
            new ProducerRecord<>(cmd, message)
        );
        sut.close();
    }
}
