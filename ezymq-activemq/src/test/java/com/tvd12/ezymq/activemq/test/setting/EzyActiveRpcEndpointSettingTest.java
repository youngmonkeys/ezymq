package com.tvd12.ezymq.activemq.test.setting;

import com.tvd12.ezymq.activemq.setting.EzyActiveRpcEndpointSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.Destination;

import static org.mockito.Mockito.mock;

public class EzyActiveRpcEndpointSettingTest extends BaseTest {

    @Test
    public void test() {
        // given
        int threadPoolSize = RandomUtil.randomSmallInt() + 1;
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        String requestQueueName = RandomUtil.randomShortAlphabetString();
        String replyQueueName = RandomUtil.randomShortAlphabetString();

        // when
        EzyActiveRpcEndpointSetting sut = new InternalBuilder()
            .threadPoolSize(0)
            .threadPoolSize(threadPoolSize)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .requestQueueName(requestQueueName)
            .requestQueueName(requestQueueName)
            .replyQueueName(replyQueueName)
            .replyQueueName(replyQueueName)
            .build();

        // then
        Asserts.assertEquals(sut.getThreadPoolSize(), threadPoolSize);
        Asserts.assertEquals(sut.getRequestQueue(), requestQueue);
        Asserts.assertEquals(sut.getReplyQueue(), replyQueue);
        Asserts.assertEquals(sut.getRequestQueueName(), requestQueueName);
        Asserts.assertEquals(sut.getReplyQueueName(), replyQueueName);
    }

    private static class InternalBuilder
        extends EzyActiveRpcEndpointSetting.Builder<InternalBuilder> {

        @Override
        public EzyActiveRpcEndpointSetting build() {
            return new EzyActiveRpcEndpointSetting(
                session,
                requestQueueName,
                requestQueue,
                replyQueueName,
                replyQueue,
                threadPoolSize
            );
        }
    }
}
