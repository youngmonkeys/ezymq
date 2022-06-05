package com.tvd12.ezymq.activemq.test.handler;

import com.tvd12.ezymq.activemq.handler.EzyActiveRequestLogInterceptor;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyActiveRequestLogInterceptorTest extends BaseTest {

    @Test
    public void test() {
        // given
        String cmd = RandomUtil.randomShortAlphabetString();
        String request = RandomUtil.randomShortAlphabetString();
        String response = RandomUtil.randomShortAlphabetString();
        RuntimeException exception = new RuntimeException("test");

        EzyActiveRequestLogInterceptor sut = new EzyActiveRequestLogInterceptor();

        // when
        // then
        sut.preHandle(cmd, request);
        sut.postHandle(cmd, request, response);
        sut.postHandle(cmd, request, exception);
    }
}
