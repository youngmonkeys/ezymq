package com.tvd12.ezymq.mosquitto.test.setting;

import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoCorrelationIdFactory;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoResponseConsumer;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoRpcProducerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class EzyMosquittoRpcProducerSettingTest extends BaseTest {

    @Test
    public void test() {
        // given
        int capacity = RandomUtil.randomInt(10, 20);
        int defaultTimeout = RandomUtil.randomInt(1000, 2000);

        EzyMosquittoCorrelationIdFactory correlationIdFactory =
            mock(EzyMosquittoCorrelationIdFactory.class);
        EzyMosquittoResponseConsumer responseConsumer =
            mock(EzyMosquittoResponseConsumer.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        // when
        EzyMosquittoRpcProducerSetting sut = EzyMosquittoRpcProducerSetting.builder()
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .unconsumedResponseConsumer(responseConsumer)
            .build();

        // then
        Asserts.assertEquals(sut.getTopic(), topic);
        Asserts.assertEquals(sut.getReplyTopic(), replyTopic);
        Asserts.assertEquals(sut.getCapacity(), capacity);
        Asserts.assertEquals(sut.getDefaultTimeout(), defaultTimeout);
        Asserts.assertEquals(sut.getCorrelationIdFactory(), correlationIdFactory);
        Asserts.assertEquals(sut.getUnconsumedResponseConsumer(), responseConsumer);
    }
}
