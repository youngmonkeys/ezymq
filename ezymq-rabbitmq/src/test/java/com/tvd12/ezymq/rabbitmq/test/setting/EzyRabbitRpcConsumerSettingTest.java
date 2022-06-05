package com.tvd12.ezymq.rabbitmq.test.setting;

import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptors;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcConsumerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class EzyRabbitRpcConsumerSettingTest extends BaseTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void test() {
        // given
        EzyRabbitRequestInterceptor interceptor = mock(EzyRabbitRequestInterceptor.class);
        String cmd = RandomUtil.randomShortAlphabetString();
        EzyRabbitRequestHandler handler = mock(EzyRabbitRequestHandler.class);

        // when
        EzyRabbitRpcConsumerSetting sut = EzyRabbitRpcConsumerSetting.builder()
            .addRequestInterceptor(interceptor)
            .addRequestHandler(cmd, handler)
            .build();

        // then
        EzyRabbitRequestHandlers expectedHandlers = new EzyRabbitRequestHandlers();
        expectedHandlers.addHandler(cmd, handler);
        Asserts.assertEquals(
            sut.getRequestHandlers(),
            expectedHandlers
        );

        EzyRabbitRequestInterceptors expectedInterceptors = new EzyRabbitRequestInterceptors();
        expectedInterceptors.addInterceptor(interceptor);
        Asserts.assertEquals(
            sut.getRequestInterceptors(),
            expectedInterceptors
        );
    }
}
