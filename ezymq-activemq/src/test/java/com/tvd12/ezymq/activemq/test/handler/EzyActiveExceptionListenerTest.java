package com.tvd12.ezymq.activemq.test.handler;

import com.tvd12.ezymq.activemq.handler.EzyActiveExceptionListener;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import javax.jms.JMSException;

public class EzyActiveExceptionListenerTest extends BaseTest {

    @Test
    public void test() {
        // given
        JMSException exception = new JMSException("test");
        EzyActiveExceptionListener listener = new EzyActiveExceptionListener();

        // when
        // then
        listener.onException(exception);
    }
}
