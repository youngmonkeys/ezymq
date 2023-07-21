package com.tvd12.ezymq.mosquitto.test.factory;

import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoSimpleCorrelationIdFactory;
import com.tvd12.test.assertion.Asserts;
import org.testng.annotations.Test;

public class EzyMosquittoSimpleCorrelationIdFactoryTest {

    @Test
    public void newCorrelationIdTest() {
        // given
        EzyMosquittoSimpleCorrelationIdFactory instance =
            new EzyMosquittoSimpleCorrelationIdFactory();

        // when
        String actual = instance.newCorrelationId("test");

        // then
        Asserts.assertEquals(actual, "test1");
    }
}
