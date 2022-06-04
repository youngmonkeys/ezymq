package com.tvd12.ezymq.kafka.test.serialization;

import com.tvd12.ezymq.kafka.serialization.EzyDefaultSerializer;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyDefaultSerializerTest extends BaseTest {

    @Test
    public void serializeByBytesTest() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        Object object = RandomUtil.randomShortAlphabetString().getBytes();
        EzyDefaultSerializer serializer = new EzyDefaultSerializer();

        // when
        byte[] bytes = serializer.serialize(topic, object);

        // then
        Asserts.assertEquals(bytes, object);
        serializer.close();
    }

    @Test
    public void serializeByStringTest() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        Object object = RandomUtil.randomShortAlphabetString();
        EzyDefaultSerializer serializer = new EzyDefaultSerializer();

        // when
        byte[] bytes = serializer.serialize(topic, object);

        // then
        Asserts.assertEquals(bytes, object.toString().getBytes());
        serializer.close();
    }
}
