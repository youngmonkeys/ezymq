package com.tvd12.ezymq.rabbitmq.util;

import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitMessageConsume;

public final class EzyRabbitMessageConsumeAnnotations {

	private EzyRabbitMessageConsumeAnnotations() {}
	
	public static String getCommand(EzyRabbitMessageConsume annotation) {
		String cmd = annotation.value();
		if(cmd.isEmpty())
			cmd = annotation.cmd();
		return cmd;
	}
	
}
