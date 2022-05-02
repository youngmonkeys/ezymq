package com.tvd12.ezymq.rabbitmq.factory;

import com.tvd12.ezyfox.util.EzyLoggable;

import java.util.concurrent.atomic.AtomicLong;

public class EzyRabbitSimpleCorrelationIdFactory
    extends EzyLoggable
    implements EzyRabbitCorrelationIdFactory {

    private final String prefix;
    private final AtomicLong generator = new AtomicLong();

    public EzyRabbitSimpleCorrelationIdFactory() {
        this("");
    }

    public EzyRabbitSimpleCorrelationIdFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String newCorrelationId() {
        return prefix + generator.incrementAndGet();
    }
}
