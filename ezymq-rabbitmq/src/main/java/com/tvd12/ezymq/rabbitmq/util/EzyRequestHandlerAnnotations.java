package com.tvd12.ezymq.rabbitmq.util;

import com.tvd12.ezymq.rabbitmq.annotation.EzyRequestHandler;

public final class EzyRequestHandlerAnnotations {

	private EzyRequestHandlerAnnotations() {
	}
	
	public static String getCommand(EzyRequestHandler annotation) {
		String cmd = annotation.value();
		if(cmd.isEmpty())
			cmd = annotation.cmd();
		return cmd;
	}
	
}
