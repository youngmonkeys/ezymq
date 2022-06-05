package com.tvd12.ezymq.activemq.test.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyActiveRequestHandlerTest {

    @Test
    public void handleTest() throws Exception {
        // given
        EzyActiveRequestHandler<String> sut = new InternalMQMessageConsumer();

        // when
        // then
        String message = RandomUtil.randomShortAlphabetString();
        sut.handle(message);
    }

    @Test
    public void getRequestTypeTest() {
        // given
        EzyActiveRequestHandler<String> sut = new InternalMQMessageConsumer();

        // when
        Class<?> requestType = sut.getRequestType();

        // then
        Asserts.assertEquals(requestType, String.class);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void getRequestTypeFailedTest() {
        // given
        EzyActiveRequestHandler sut = new EzyActiveRequestHandler() {
            @Override
            public void process(Object message) {}
        };

        // when
        Throwable e = Asserts.assertThrows(sut::getRequestType);

        // then
        Asserts.assertEqualsType(e, EzyNotImplementedException.class);
    }

    public static class InternalMQMessageConsumer
        implements EzyActiveRequestHandler<String> {
    }
}
