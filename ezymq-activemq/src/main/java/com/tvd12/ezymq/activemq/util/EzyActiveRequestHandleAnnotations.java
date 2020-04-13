package com.tvd12.ezymq.activemq.util;

import com.tvd12.ezymq.activemq.annotation.EzyActiveRequestHandle;

public final class EzyActiveRequestHandleAnnotations {

	private EzyActiveRequestHandleAnnotations() {}
	
	public static String getCommand(EzyActiveRequestHandle annotation) {
		String cmd = annotation.value();
		if(cmd.isEmpty())
			cmd = annotation.cmd();
		return cmd;
	}
	
}
