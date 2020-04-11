package com.tvd12.ezymq.rabbitmq.factory;

import java.util.concurrent.atomic.AtomicLong;

import com.tvd12.ezyfox.util.EzyLoggable;

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
		return new StringBuilder()
				.append(prefix)
				.append(generator.incrementAndGet())
				.toString();
	}
	
}
