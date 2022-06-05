package com.tvd12.ezymq.activemq.test.constant;

import com.tvd12.ezymq.activemq.constant.EzyActiveDestinationType;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

public class EzyActiveDestinationTypeTest extends BaseTest {

    @Test
    public void test() {
        Asserts.assertEquals(
            EzyActiveDestinationType.QUEUE.getId(),
            1
        );
    }
}
