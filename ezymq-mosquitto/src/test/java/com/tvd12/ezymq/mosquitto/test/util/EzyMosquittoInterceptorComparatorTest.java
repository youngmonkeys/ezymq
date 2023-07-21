package com.tvd12.ezymq.mosquitto.test.util;

import com.tvd12.ezymq.common.handler.EzyMQRequestLogInterceptor;
import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoInterceptor;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoInterceptorComparator;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

public class EzyMosquittoInterceptorComparatorTest extends BaseTest {

    @Test
    public void compareStringTest() {
        // given
        // when
        int result = EzyMosquittoInterceptorComparator.getInstance()
            .compare("a", "b");

        // then
        Asserts.assertEquals(result, 0);
    }

    @Test
    public void compareInterceptorTest() {
        // given
        // when
        int result = EzyMosquittoInterceptorComparator.getInstance()
            .compare(new Interceptor1(), new Interceptor2());

        // then
        Asserts.assertEquals(result, -1);
    }

    @EzyMosquittoInterceptor(priority = 1)
    public static class Interceptor1 extends EzyMQRequestLogInterceptor {}

    @EzyMosquittoInterceptor(priority = 2)
    public static class Interceptor2 extends EzyMQRequestLogInterceptor {}
}
