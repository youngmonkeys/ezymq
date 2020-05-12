package com.tvd12.ezymq.activemq.setting;

import javax.jms.Session;

import lombok.Getter;

@Getter
public class EzyActiveEndpointSetting {

	protected final Session session;
	
	public EzyActiveEndpointSetting(Session session) {
		this.session = session;
	}
	
}
