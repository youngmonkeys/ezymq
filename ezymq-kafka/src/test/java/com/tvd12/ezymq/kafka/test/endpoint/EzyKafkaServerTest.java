package com.tvd12.ezymq.kafka.test.endpoint;

import com.tvd12.ezyfox.util.EzyThreads;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRecordsHandler;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.ReflectMethodUtil;
import com.tvd12.test.util.RandomUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;
import org.testng.annotations.Test;

import java.time.Duration;

import static org.mockito.Mockito.*;

public class EzyKafkaServerTest extends BaseTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void test() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        Consumer consumer = mock(Consumer.class);
        long pollTimeOut = 10;

        EzyKafkaServer sut = new EzyKafkaServer(
            topic,
            consumer,
            pollTimeOut
        );

        ConsumerRecord record = mock(ConsumerRecord.class);
        EzyKafkaRecordsHandler recordsHandler = mock(EzyKafkaRecordsHandler.class);
        sut.setRecordsHandler(recordsHandler);

        ConsumerRecords records = mock(ConsumerRecords.class);
        doAnswer(it -> {
            java.util.function.Consumer func = it.getArgumentAt(
                0,
                java.util.function.Consumer.class
            );
            func.accept(record);
            return null;
        }).when(records).forEach(any(java.util.function.Consumer.class));
        when(
            consumer.poll(Duration.ofMillis(pollTimeOut))
        ).thenReturn(records);

        // when
        sut.start();
        EzyThreads.sleep(100);
        sut.close();

        // then
        Throwable e = Asserts.assertThrows(sut::start);
        Asserts.assertEqualsType(e, IllegalStateException.class);

        verify(
            consumer,
            atLeast(1)
        ).poll(Duration.ofMillis(pollTimeOut));
        verify(
            records,
            atLeast(1)
        ).forEach(any(java.util.function.Consumer.class));
        verify(
            recordsHandler,
            atLeast(1)
        ).handleRecord(record);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void pollRecordsFailedDueToRecordsHandler() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        Consumer consumer = mock(Consumer.class);
        long pollTimeOut = 10;

        EzyKafkaServer sut = EzyKafkaServer.builder()
            .topic(topic)
            .consumer(consumer)
            .pollTimeOut(pollTimeOut)
            .build();

        ConsumerRecord record = mock(ConsumerRecord.class);
        EzyKafkaRecordsHandler recordsHandler = mock(EzyKafkaRecordsHandler.class);
        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(recordsHandler).handleRecord(record);
        sut.setRecordsHandler(recordsHandler);

        ConsumerRecords records = mock(ConsumerRecords.class);
        doAnswer(it -> {
            java.util.function.Consumer func = it.getArgumentAt(
                0,
                java.util.function.Consumer.class
            );
            func.accept(record);
            return null;
        }).when(records).forEach(any(java.util.function.Consumer.class));
        when(
            consumer.poll(Duration.ofMillis(pollTimeOut))
        ).thenReturn(records);

        // when
        sut.start();
        EzyThreads.sleep(10);
        sut.close();

        // then
        Throwable e = Asserts.assertThrows(sut::start);
        Asserts.assertEqualsType(e, IllegalStateException.class);

        verify(
            consumer,
            atLeast(1)
        ).poll(Duration.ofMillis(pollTimeOut));
        verify(
            records,
            atLeast(1)
        ).forEach(any(java.util.function.Consumer.class));
        verify(
            recordsHandler,
            atLeast(1)
        ).handleRecord(record);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void pollRecordsFailedDueToPoll() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        Consumer consumerTest = mock(Consumer.class);
        long pollTimeOut = 10;

        Deserializer deserializer = mock(Deserializer.class);
        EzyKafkaServer sut = new EzyKafkaServer.Builder() {
            @Override
            protected Consumer newConsumer(Deserializer deserializer) {
                return consumerTest;
            }
        }
            .topic(topic)
            .pollTimeOut(pollTimeOut)
            .deserializer(deserializer)
            .build();

        EzyKafkaRecordsHandler recordsHandler = mock(EzyKafkaRecordsHandler.class);
        sut.setRecordsHandler(recordsHandler);

        RuntimeException exception = new RuntimeException("test");
        when(
            consumerTest.poll(Duration.ofMillis(pollTimeOut))
        ).thenThrow(exception);

        // when
        sut.start();
        EzyThreads.sleep(10);
        sut.close();
        ReflectMethodUtil.invokeMethod("pollRecords", sut);

        // then
        Throwable e = Asserts.assertThrows(sut::start);
        Asserts.assertEqualsType(e, IllegalStateException.class);

        verify(
            consumerTest,
            atLeast(1)
        ).poll(Duration.ofMillis(pollTimeOut));
    }
}
