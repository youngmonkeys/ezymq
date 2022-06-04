package com.tvd12.ezymq.common.test.handler;

import com.tvd12.ezyfox.exception.EzyNotImplementedException;
import com.tvd12.ezymq.common.handler.EzyMQRequestHandler;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyMQRequestHandlerTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void handleTest() throws Exception {
        // given
        String request = RandomUtil.randomShortAlphabetString();
        EzyMQRequestHandler sut = new EzyMQRequestHandler() {};

        // when
        Object result = sut.handle(request);

        // then
        Asserts.assertEquals(result, Boolean.TRUE);
    }

    @Test
    public void getRequestTypeTest() {
        // given
        EzyMQRequestHandler<String> sut = new EzyMQRequestHandler<String>() {};

        // when
        Class<?> requestType = sut.getRequestType();

        // then
        Asserts.assertEquals(requestType, String.class);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void getRequestTypeFailedTest() {
        // given
        EzyMQRequestHandler sut = new EzyMQRequestHandler() {};

        // when
        Throwable e = Asserts.assertThrows(sut::getRequestType);

        // then
        Asserts.assertEqualsType(e, EzyNotImplementedException.class);
    }
}
