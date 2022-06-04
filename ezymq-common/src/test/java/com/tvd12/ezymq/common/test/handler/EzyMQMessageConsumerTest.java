package com.tvd12.ezymq.common.test.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.test.assertion.Asserts;
import org.testng.annotations.Test;

public class EzyMQMessageConsumerTest {

    @Test
    public void getRequestTypeTest() {
        // given
        EzyMQMessageConsumer<String> sut = new InternalMQMessageConsumer();

        // when
        Class<?> requestType = sut.getMessageType();

        // then
        Asserts.assertEquals(requestType, String.class);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void getRequestTypeFailedTest() {
        // given
        EzyMQMessageConsumer sut = message -> {};

        // when
        Throwable e = Asserts.assertThrows(sut::getMessageType);

        // then
        Asserts.assertEqualsType(e, EzyNotImplementedException.class);
    }

    public static class InternalMQMessageConsumer
        implements EzyMQMessageConsumer<String> {

        @Override
        public void consume(String message) {}
    }
}
