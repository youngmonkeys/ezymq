package com.tvd12.ezymq.activemq.test.factory;

import com.tvd12.ezymq.activemq.factory.EzyActiveSimpleCorrelationIdFactory;
import com.tvd12.test.assertion.Asserts;
import org.testng.annotations.Test;

public class EzyActiveSimpleCorrelationIdFactoryTest {

    @Test
    public void newCorrelationIdTest() {
        // given
        EzyActiveSimpleCorrelationIdFactory instance =
            new EzyActiveSimpleCorrelationIdFactory("test");

        // when
        String actual = instance.newCorrelationId();

        // then
        Asserts.assertEquals(actual, "test1");
    }
}
