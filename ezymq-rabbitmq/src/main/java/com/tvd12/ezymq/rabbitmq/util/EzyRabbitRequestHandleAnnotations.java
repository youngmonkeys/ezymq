package com.tvd12.ezymq.rabbitmq.util;

import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitRequestHandle;

public final class EzyRabbitRequestHandleAnnotations {

	private EzyRabbitRequestHandleAnnotations() {}
	
	public static String getCommand(EzyRabbitRequestHandle annotation) {
		String cmd = annotation.value();
		if(cmd.isEmpty())
			cmd = annotation.cmd();
		return cmd;
	}
	
}
