package com.tvd12.ezymq.rabbitmq.test.util;

import com.tvd12.ezymq.common.handler.EzyMQRequestLogInterceptor;
import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitInterceptor;
import com.tvd12.ezymq.rabbitmq.util.EzyRabbitInterceptorComparator;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

public class EzyRabbitInterceptorComparatorTest extends BaseTest {

    @Test
    public void compareStringTest() {
        // given
        // when
        int result = EzyRabbitInterceptorComparator.getInstance()
            .compare("a", "b");

        // then
        Asserts.assertEquals(result, 0);
    }

    @Test
    public void compareInterceptorTest() {
        // given
        // when
        int result = EzyRabbitInterceptorComparator.getInstance()
            .compare(new Interceptor1(), new Interceptor2());

        // then
        Asserts.assertEquals(result, -1);
    }

    @EzyRabbitInterceptor(priority = 1)
    public static class Interceptor1 extends EzyMQRequestLogInterceptor {}

    @EzyRabbitInterceptor(priority = 2)
    public static class Interceptor2 extends EzyMQRequestLogInterceptor {}
}
