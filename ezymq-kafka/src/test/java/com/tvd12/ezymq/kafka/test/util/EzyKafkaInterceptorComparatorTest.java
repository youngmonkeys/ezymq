package com.tvd12.ezymq.kafka.test.util;

import com.tvd12.ezymq.kafka.annotation.EzyKafkaInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.util.EzyKafkaInterceptorComparator;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

public class EzyKafkaInterceptorComparatorTest extends BaseTest {

    @Test
    public void compareStringTest() {
        // given
        // when
        int result = EzyKafkaInterceptorComparator.getInstance()
            .compare("a", "b");

        // then
        Asserts.assertEquals(result, 0);
    }

    @Test
    public void compareInterceptorTest() {
        // given
        // when
        int result = EzyKafkaInterceptorComparator.getInstance()
            .compare(new Interceptor1(), new Interceptor2());

        // then
        Asserts.assertEquals(result, -1);
    }

    @EzyKafkaInterceptor(priority = 1)
    public static class Interceptor1 implements EzyKafkaMessageInterceptor {}

    @EzyKafkaInterceptor(priority = 2)
    public static class Interceptor2 implements EzyKafkaMessageInterceptor {}
}
