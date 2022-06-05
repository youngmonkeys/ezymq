package com.tvd12.ezymq.activemq.test.setting;

import com.tvd12.ezymq.activemq.factory.EzyActiveCorrelationIdFactory;
import com.tvd12.ezymq.activemq.handler.EzyActiveResponseConsumer;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcProducerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.Session;

import static org.mockito.Mockito.mock;

public class EzyActiveRpcProducerSettingTest extends BaseTest {

    @Test
    public void test() {
        // given
        int capacity = RandomUtil.randomInt(10, 20);
        int defaultTimeout = RandomUtil.randomInt(1000, 2000);

        EzyActiveCorrelationIdFactory correlationIdFactory =
            mock(EzyActiveCorrelationIdFactory.class);
        EzyActiveResponseConsumer responseConsumer =
            mock(EzyActiveResponseConsumer.class);

        Session session = mock(Session.class);
        String requestQueue = RandomUtil.randomShortAlphabetString();
        String replyQueue = RandomUtil.randomShortAlphabetString();

        // when
        EzyActiveRpcProducerSetting sut = EzyActiveRpcProducerSetting.builder()
            .requestQueueName(requestQueue)
            .replyQueueName(replyQueue)
            .session(session)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .unconsumedResponseConsumer(responseConsumer)
            .build();

        // then
        Asserts.assertEquals(sut.getCapacity(), capacity);
        Asserts.assertEquals(sut.getDefaultTimeout(), defaultTimeout);
        Asserts.assertEquals(sut.getCorrelationIdFactory(), correlationIdFactory);
        Asserts.assertEquals(sut.getUnconsumedResponseConsumer(), responseConsumer);
    }
}
