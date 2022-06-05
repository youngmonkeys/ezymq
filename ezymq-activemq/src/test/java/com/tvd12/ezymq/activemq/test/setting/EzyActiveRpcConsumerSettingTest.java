package com.tvd12.ezymq.activemq.test.setting;

import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptor;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptors;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcConsumerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class EzyActiveRpcConsumerSettingTest extends BaseTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void test() {
        // given
        EzyActiveRequestInterceptor interceptor = mock(EzyActiveRequestInterceptor.class);
        String cmd = RandomUtil.randomShortAlphabetString();
        EzyActiveRequestHandler handler = mock(EzyActiveRequestHandler.class);

        // when
        EzyActiveRpcConsumerSetting sut = EzyActiveRpcConsumerSetting.builder()
            .addRequestInterceptor(interceptor)
            .addRequestHandler(cmd, handler)
            .build();

        // then
        EzyActiveRequestHandlers expectedHandlers = new EzyActiveRequestHandlers();
        expectedHandlers.addHandler(cmd, handler);
        Asserts.assertEquals(
            sut.getRequestHandlers(),
            expectedHandlers
        );

        EzyActiveRequestInterceptors expectedInterceptors = new EzyActiveRequestInterceptors();
        expectedInterceptors.addInterceptor(interceptor);
        Asserts.assertEquals(
            sut.getRequestInterceptors(),
            expectedInterceptors
        );
    }
}
