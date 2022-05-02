package com.tvd12.ezymq.activemq.handler;

import com.tvd12.ezyfox.util.EzyLoggable;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

public class EzyActiveExceptionListener
    extends EzyLoggable
    implements ExceptionListener {

    @Override
    public void onException(JMSException exception) {
        logger.warn("has an exception", exception);
    }
}
