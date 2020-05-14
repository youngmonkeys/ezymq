package com.tvd12.ezymq.activemq.setting;

import javax.jms.Session;

import com.tvd12.ezyfox.builder.EzyBuilder;

import lombok.Getter;

@Getter
public class EzyActiveEndpointSetting {

	protected final Session session;
	
	public EzyActiveEndpointSetting(Session session) {
		this.session = session;
	}
	
	@SuppressWarnings("unchecked")
	public static abstract class Builder<B extends Builder<B>> 
			implements EzyBuilder<EzyActiveEndpointSetting> {
		
		protected Session session; 
		
		public B session(Session session) {
			this.session = session;
			return (B)this;
		}

	}
	
}
