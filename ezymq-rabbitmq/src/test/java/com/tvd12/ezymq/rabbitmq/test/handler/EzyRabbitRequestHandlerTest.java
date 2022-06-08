package com.tvd12.ezymq.rabbitmq.test.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyRabbitRequestHandlerTest {

    @Test
    public void handleTest() throws Exception {
        // given
        EzyRabbitRequestHandler<String> sut = new InternalMQMessageConsumer();

        // when
        // then
        String message = RandomUtil.randomShortAlphabetString();
        sut.handle(message);
    }

    @Test
    public void getRequestTypeTest() {
        // given
        EzyRabbitRequestHandler<String> sut = new InternalMQMessageConsumer();

        // when
        Class<?> requestType = sut.getRequestType();

        // then
        Asserts.assertEquals(requestType, String.class);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void getRequestTypeFailedTest() {
        // given
        EzyRabbitRequestHandler sut = new EzyRabbitRequestHandler() {
            @Override
            public void process(Object message) {}
        };

        // when
        Throwable e = Asserts.assertThrows(sut::getRequestType);

        // then
        Asserts.assertEqualsType(e, EzyNotImplementedException.class);
    }

    public static class InternalMQMessageConsumer
        implements EzyRabbitRequestHandler<String> {
    }
}
