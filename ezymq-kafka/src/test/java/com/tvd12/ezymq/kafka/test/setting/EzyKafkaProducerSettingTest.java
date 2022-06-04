package com.tvd12.ezymq.kafka.test.setting;

import com.tvd12.ezyfox.collect.Sets;
import com.tvd12.ezymq.kafka.setting.EzyKafkaProducerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.apache.kafka.clients.producer.Producer;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.mockito.Mockito.mock;

public class EzyKafkaProducerSettingTest extends BaseTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void test() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        Producer producer = mock(Producer.class);
        String propertyName1 = RandomUtil.randomShortAlphabetString();
        String propertyValue1 = RandomUtil.randomShortAlphabetString();
        String propertyName2 = RandomUtil.randomShortAlphabetString();
        String propertyValue2 = RandomUtil.randomShortAlphabetString();

        // when
        EzyKafkaProducerSetting sut = EzyKafkaProducerSetting.builder()
            .topic(topic)
            .producer(producer)
            .property(propertyName1, propertyValue1)
            .properties(null)
            .properties(Collections.singletonMap(propertyName2, propertyValue2))
            .build();

        // then
        Asserts.assertEquals(sut.getTopic(), topic);
        Asserts.assertEquals(sut.getProducer(), producer);

        Asserts.assertTrue(sut.containsProperty(propertyName1));
        Asserts.assertFalse(sut.containsProperty("unknown key"));
        Asserts.assertTrue(
            sut.getProperties()
                .keySet()
                .containsAll(
                    Sets.newHashSet(propertyName1, propertyName2)
                )
        );
        Asserts.assertTrue(
            sut.getProperties()
                .values()
                .containsAll(
                    Sets.newHashSet(propertyValue1, propertyValue2)
                )
        );
    }

    @Test
    public void buildFailedDueToTopicIsNullTest() {
        // given
        // when
        Throwable e = Asserts.assertThrows(() ->
            EzyKafkaProducerSetting.builder()
                .build()
        );

        // then
        Asserts.assertEqualsType(e, NullPointerException.class);
    }
}
