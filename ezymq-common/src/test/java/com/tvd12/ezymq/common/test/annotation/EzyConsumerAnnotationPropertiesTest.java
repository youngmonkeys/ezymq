package com.tvd12.ezymq.common.test.annotation;

import com.tvd12.ezymq.common.annotation.EzyConsumerAnnotationProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyConsumerAnnotationPropertiesTest extends BaseTest {

    @Test
    public void test() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        String command = RandomUtil.randomShortAlphabetString();

        // when
        EzyConsumerAnnotationProperties sut = new EzyConsumerAnnotationProperties(
            topic,
            command
        );

        // then
        Asserts.assertEquals(sut.getTopic(), topic);
        Asserts.assertEquals(sut.getCommand(), command);
    }
}
