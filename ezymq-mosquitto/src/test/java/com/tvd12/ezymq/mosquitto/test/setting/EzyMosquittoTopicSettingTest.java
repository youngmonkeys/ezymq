package com.tvd12.ezymq.mosquitto.test.setting;

import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoTopicSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EzyMosquittoTopicSettingTest extends BaseTest {

    @Test
    @SuppressWarnings("rawtypes")
    public void test() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        boolean producerEnable = RandomUtil.randomBoolean();
        boolean consumerEnable = RandomUtil.randomBoolean();
        Map<String, List<EzyMQMessageConsumer>> messageConsumersByTopic =
            Collections.emptyMap();

        // when
        EzyMosquittoTopicSetting sut = EzyMosquittoTopicSetting.builder()
            .topic(topic)
            .producerEnable(producerEnable)
            .consumerEnable(consumerEnable)
            .messageConsumersByTopic(messageConsumersByTopic)
            .build();

        // then
        Asserts.assertEquals(sut.getTopic(), topic);
        Asserts.assertEquals(sut.isConsumerEnable(), consumerEnable);
        Asserts.assertEquals(sut.isProducerEnable(), producerEnable);
        Asserts.assertEquals(sut.getMessageConsumersByTopic(), messageConsumersByTopic);
    }
}
