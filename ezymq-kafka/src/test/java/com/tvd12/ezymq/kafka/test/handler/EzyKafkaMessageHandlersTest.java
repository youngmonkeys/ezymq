package com.tvd12.ezymq.kafka.test.handler;

import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandlers;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyKafkaMessageHandlersTest extends BaseTest {

    @Test
    public void handleFailedDueToNoHandler() {
        // given
        EzyKafkaMessageHandlers sut = new EzyKafkaMessageHandlers();

        // when
        String cmd = RandomUtil.randomShortAlphabetString();
        String message = RandomUtil.randomShortAlphabetString();
        Throwable e = Asserts.assertThrows(() ->
            sut.handle(cmd, message)
        );

        // then
        Asserts.assertEqualsType(e, IllegalArgumentException.class);
    }
}
