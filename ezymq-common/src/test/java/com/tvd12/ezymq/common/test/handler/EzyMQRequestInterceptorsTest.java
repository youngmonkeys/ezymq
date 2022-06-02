package com.tvd12.ezymq.common.test.handler;

import com.tvd12.ezymq.common.handler.EzyMQRequestInterceptors;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.Mockito.*;

public class EzyMQRequestInterceptorsTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyTestMQRequestInterceptor interceptor1 = mock(EzyTestMQRequestInterceptor.class);
        EzyTestMQRequestInterceptor interceptor2 = mock(EzyTestMQRequestInterceptor.class);
        EzyTestMQRequestInterceptor interceptor3 = mock(EzyTestMQRequestInterceptor.class);

        InternalInterceptors sut = new InternalInterceptors();
        sut.addInterceptor(interceptor1);
        sut.addInterceptors(
            Arrays.asList(interceptor2, interceptor3)
        );

        // when
        String cmd = RandomUtil.randomShortAlphabetString();
        String message = RandomUtil.randomShortAlphabetString();
        String result = RandomUtil.randomShortAlphabetString();
        Throwable e = new RuntimeException("test");
        sut.preHandle(cmd, message);
        sut.postHandle(cmd, message, result);
        sut.postHandle(cmd, message, e);

        // then
        verify(interceptor1, times(1)).preHandle(cmd, message);
        verify(interceptor2, times(1)).preHandle(cmd, message);
        verify(interceptor3, times(1)).preHandle(cmd, message);
        verify(interceptor1, times(1)).postHandle(cmd, message, result);
        verify(interceptor2, times(1)).postHandle(cmd, message, result);
        verify(interceptor3, times(1)).postHandle(cmd, message, result);
        verify(interceptor1, times(1)).postHandle(cmd, message, e);
        verify(interceptor2, times(1)).postHandle(cmd, message, e);
        verify(interceptor3, times(1)).postHandle(cmd, message, e);
    }

    private static class InternalInterceptors
        extends EzyMQRequestInterceptors<EzyTestMQRequestInterceptor> {
    }
}
