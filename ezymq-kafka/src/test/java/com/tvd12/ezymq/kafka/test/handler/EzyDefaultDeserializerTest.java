package com.tvd12.ezymq.kafka.test.handler;

import com.tvd12.ezymq.kafka.serialization.EzyDefaultDeserializer;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyDefaultDeserializerTest extends BaseTest {

    @Test
    public void test() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        byte[] data = RandomUtil.randomShortByteArray();
        EzyDefaultDeserializer sut = new EzyDefaultDeserializer();

        // when
        Object result = sut.deserialize(topic, data);

        // then
        Asserts.assertEquals(result, data);
        sut.close();
    }
}
