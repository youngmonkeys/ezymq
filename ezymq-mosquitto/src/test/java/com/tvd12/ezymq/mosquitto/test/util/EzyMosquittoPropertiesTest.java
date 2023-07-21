package com.tvd12.ezymq.mosquitto.test.util;

import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Map;

public class EzyMosquittoPropertiesTest {

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void toStringTest() {
        // given
        EzyMosquittoProperties instance = EzyMosquittoProperties.builder()
            .messageId(RandomUtil.randomSmallInt())
            .messageType(RandomUtil.randomShortAlphabetString())
            .correlationId(RandomUtil.randomShortAlphabetString())
            .qos(RandomUtil.randomInt())
            .retained(RandomUtil.randomBoolean())
            .headers((Map) RandomUtil.randomMap(1, String.class, String.class))
            .build();

        // when
        // then
        System.out.println(instance);
    }
}
