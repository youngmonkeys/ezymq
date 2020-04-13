package com.tvd12.ezymq.activemq.factory;

import java.util.concurrent.atomic.AtomicLong;

import com.tvd12.ezyfox.util.EzyLoggable;

public class EzyActiveSimpleCorrelationIdFactory
		extends EzyLoggable
		implements EzyActiveCorrelationIdFactory {

	private final String prefix;
	private final AtomicLong generator = new AtomicLong();
	
	public EzyActiveSimpleCorrelationIdFactory() {
		this("");
	}
	
	public EzyActiveSimpleCorrelationIdFactory(String prefix) {
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
