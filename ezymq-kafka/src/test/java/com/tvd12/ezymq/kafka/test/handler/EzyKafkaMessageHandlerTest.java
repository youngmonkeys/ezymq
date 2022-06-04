package com.tvd12.ezymq.kafka.test.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyKafkaMessageHandlerTest {

    @Test
    public void handleTest() throws Exception {
        // given
        EzyKafkaMessageHandler<String> sut = new InternalMQMessageConsumer();

        // when
        // then
        String message = RandomUtil.randomShortAlphabetString();
        sut.handle(message);
    }

    @Test
    public void getRequestTypeTest() {
        // given
        EzyKafkaMessageHandler<String> sut = new InternalMQMessageConsumer();

        // when
        Class<?> requestType = sut.getMessageType();

        // then
        Asserts.assertEquals(requestType, String.class);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void getRequestTypeFailedTest() {
        // given
        EzyKafkaMessageHandler sut = new EzyKafkaMessageHandler() {
            @Override
            public void process(Object message) {}
        };

        // when
        Throwable e = Asserts.assertThrows(sut::getMessageType);

        // then
        Asserts.assertEqualsType(e, EzyNotImplementedException.class);
    }

    public static class InternalMQMessageConsumer
        implements EzyKafkaMessageHandler<String> {
    }
}
