package com.tvd12.ezymq.activemq.test.util;

import com.tvd12.ezymq.activemq.annotation.EzyActiveInterceptor;
import com.tvd12.ezymq.activemq.util.EzyActiveInterceptorComparator;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

public class EzyActiveInterceptorComparatorTest extends BaseTest {

    @Test
    public void compareStringTest() {
        // given
        // when
        int result = EzyActiveInterceptorComparator.getInstance()
            .compare("a", "b");

        // then
        Asserts.assertEquals(result, 0);
    }

    @Test
    public void compareInterceptorTest() {
        // given
        // when
        int result = EzyActiveInterceptorComparator.getInstance()
            .compare(new Interceptor1(), new Interceptor2());

        // then
        Asserts.assertEquals(result, -1);
    }

    @EzyActiveInterceptor(priority = 1)
    public static class Interceptor1 {}

    @EzyActiveInterceptor(priority = 2)
    public static class Interceptor2 {}
}
