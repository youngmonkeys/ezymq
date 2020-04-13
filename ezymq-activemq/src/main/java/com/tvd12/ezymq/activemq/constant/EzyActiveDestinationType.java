package com.tvd12.ezymq.activemq.constant;

import com.tvd12.ezyfox.constant.EzyConstant;

import lombok.Getter;

public enum EzyActiveDestinationType implements EzyConstant {

	QUEUE(1, "queue"),
	TOPIC(2, "topic");
	
	@Getter
	private final int id;
	@Getter
	private final String name;
	
	private EzyActiveDestinationType(int id, String name) {
		this.id = id;
		this.name = name;
	}
	
}
