package com.tvd12.ezymq.mosquitto.test.setting;

import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandlers;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestInterceptor;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestInterceptors;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoRpcConsumerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.mockito.Mockito.mock;

public class EzyMosquittoRpcConsumerSettingTest extends BaseTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void test() {
        // given
        EzyMosquittoRequestInterceptor interceptor1 = mock(EzyMosquittoRequestInterceptor.class);
        EzyMosquittoRequestInterceptor interceptor2 = mock(EzyMosquittoRequestInterceptor.class);
        String cmd1 = RandomUtil.randomShortAlphabetString();
        String cmd2 = RandomUtil.randomShortAlphabetString();
        EzyMosquittoRequestHandler handler1 = mock(EzyMosquittoRequestHandler.class);
        EzyMosquittoRequestHandler handler2 = mock(EzyMosquittoRequestHandler.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();
        int threadPoolSize = RandomUtil.randomSmallInt() + 1;

        // when
        EzyMosquittoRpcConsumerSetting sut = EzyMosquittoRpcConsumerSetting.builder()
            .topic(topic)
            .replyTopic(replyTopic)
            .threadPoolSize(threadPoolSize)
            .addRequestInterceptor(interceptor1)
            .addRequestInterceptors(Collections.singleton(interceptor2))
            .addRequestHandler(cmd1, handler1)
            .addRequestHandlers(Collections.singletonMap(cmd2, handler2))
            .build();

        // then
        EzyMosquittoRequestHandlers expectedHandlers = new EzyMosquittoRequestHandlers();
        expectedHandlers.addHandler(cmd1, handler1);
        expectedHandlers.addHandler(cmd2, handler2);
        Asserts.assertEquals(
            sut.getRequestHandlers(),
            expectedHandlers
        );

        EzyMosquittoRequestInterceptors expectedInterceptors = new EzyMosquittoRequestInterceptors();
        expectedInterceptors.addInterceptor(interceptor1);
        expectedInterceptors.addInterceptor(interceptor2);
        Asserts.assertEquals(
            sut.getRequestInterceptors(),
            expectedInterceptors
        );

        Asserts.assertEquals(sut.getTopic(), topic);
        Asserts.assertEquals(sut.getReplyTopic(), replyTopic);
        Asserts.assertEquals(sut.getThreadPoolSize(), threadPoolSize);
    }
}
