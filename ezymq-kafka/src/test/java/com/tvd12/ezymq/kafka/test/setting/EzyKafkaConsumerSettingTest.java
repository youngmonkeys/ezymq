package com.tvd12.ezymq.kafka.test.setting;

import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.setting.EzyKafkaConsumerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.mockito.Mockito.mock;

public class EzyKafkaConsumerSettingTest extends BaseTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void test() {
        // given
        long pollTimeOut = RandomUtil.randomSmallInt();
        int threadPoolSize = RandomUtil.randomSmallInt() + 1;
        Consumer consumer = mock(Consumer.class);
        EzyKafkaMessageInterceptor interceptor = mock(EzyKafkaMessageInterceptor.class);
        String command = RandomUtil.randomShortAlphabetString();
        EzyKafkaMessageHandler messageHandler = mock(EzyKafkaMessageHandler.class);
        String topic = RandomUtil.randomShortAlphabetString();

        // when
        EzyKafkaConsumerSetting sut = EzyKafkaConsumerSetting.builder()
            .topic(topic)
            .pollTimeOut(pollTimeOut)
            .threadPoolSize(0)
            .threadPoolSize(threadPoolSize)
            .consumer(consumer)
            .addMessageInterceptor(interceptor)
            .addMessageHandler(command, messageHandler)
            .build();

        // then
        Asserts.assertEquals(sut.getTopic(), topic);
        Asserts.assertEquals(sut.getPollTimeOut(), pollTimeOut);
        Asserts.assertEquals(sut.getThreadPoolSize(), threadPoolSize);
        Asserts.assertEquals(sut.getConsumer(), consumer);
        Asserts.assertEquals(
            FieldUtil.getFieldValue(
                sut.getMessageInterceptors(),
                "interceptors"
            ),
            Collections.singleton(interceptor),
            false
        );
        Asserts.assertEquals(
            sut.getMessageHandlers().getHandler(command),
            messageHandler
        );
    }

    @Test
    public void buildFailedDueToTopicIsNullTest() {
        // given
        // when
        Throwable e = Asserts.assertThrows(() ->
            EzyKafkaConsumerSetting.builder()
                .build()
        );

        // then
        Asserts.assertEqualsType(e, NullPointerException.class);
    }
}
