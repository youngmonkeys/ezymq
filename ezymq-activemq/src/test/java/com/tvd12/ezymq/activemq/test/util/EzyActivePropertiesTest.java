package com.tvd12.ezymq.activemq.test.util;

import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import static java.util.Collections.singletonMap;

public class EzyActivePropertiesTest extends BaseTest {

    @Test
    public void getValueTest() {
        // given
        EzyActiveProperties properties = EzyActiveProperties.builder()
            .addProperties(singletonMap("hello", "world"))
            .addProperties(singletonMap("foo", "bar"))
            .build();

        // when
        Object result = properties.getValue("hello");

        // then
        Asserts.assertEquals(result, "world");
        System.out.println(properties);
    }

    @Test
    public void getValueReturnNullTest() {
        // given
        EzyActiveProperties properties = EzyActiveProperties.builder()
            .addProperties((EzyActiveProperties) null)
            .build();

        // when
        Object result = properties.getValue("any");

        // then
        Asserts.assertNull(result);
        System.out.println(properties);
    }
}
