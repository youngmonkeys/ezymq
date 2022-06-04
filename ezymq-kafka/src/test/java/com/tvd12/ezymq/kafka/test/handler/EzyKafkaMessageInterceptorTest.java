package com.tvd12.ezymq.kafka.test.handler;

import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

public class EzyKafkaMessageInterceptorTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyKafkaMessageInterceptor sut = new EzyKafkaMessageInterceptor() {};

        String topic = RandomUtil.randomShortAlphabetString();
        String cmd = RandomUtil.randomShortAlphabetString();
        Object message = RandomUtil.randomShortAlphabetString();
        Object result = RandomUtil.randomShortAlphabetString();
        Throwable e = new RuntimeException("test");

        // when
        // then
        sut.preHandle(topic, cmd, message);
        sut.postHandle(topic, cmd, message, result);
        sut.postHandle(topic, cmd, message, e);
    }
}
