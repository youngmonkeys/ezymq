package com.tvd12.ezymq.mosquitto.test.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyMosquittoRequestHandlerTest extends BaseTest {

    @Test
    public void handleTest() throws Exception {
        // given
        EzyMosquittoRequestHandler<String> sut = new InternalMQMessageConsumer();

        // when
        // then
        String message = RandomUtil.randomShortAlphabetString();
        sut.handle(message);
    }

    @Test
    public void getRequestTypeTest() {
        // given
        EzyMosquittoRequestHandler<String> sut = new InternalMQMessageConsumer();

        // when
        Class<?> requestType = sut.getRequestType();

        // then
        Asserts.assertEquals(requestType, String.class);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void getRequestTypeFailedTest() {
        // given
        EzyMosquittoRequestHandler sut = new EzyMosquittoRequestHandler() {
            @Override
            public void process(Object message) {}
        };

        // when
        Throwable e = Asserts.assertThrows(sut::getRequestType);

        // then
        Asserts.assertEqualsType(e, EzyNotImplementedException.class);
    }

    public static class InternalMQMessageConsumer
        implements EzyMosquittoRequestHandler<String> {
    }
}
