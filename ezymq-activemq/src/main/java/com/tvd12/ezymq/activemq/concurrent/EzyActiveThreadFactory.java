package com.tvd12.ezymq.activemq.concurrent;

import com.tvd12.ezyfox.concurrent.EzyThreadFactory;

public class EzyActiveThreadFactory extends EzyThreadFactory {
	
	protected EzyActiveThreadFactory(Builder builder) {
		super(builder);
	}
	
	public static EzyThreadFactory create(String poolName) {
		return builder().poolName(poolName).build();
	}
	
	public static Builder builder() {
		return new Builder();
	}

	public static class Builder extends EzyThreadFactory.Builder {
		
		protected Builder() {
			super();
			this.prefix = "ezymq-activemq";
		}
		
		@Override
		public EzyThreadFactory build() {
			return new EzyActiveThreadFactory(this);
		}
		
	}
	
}
