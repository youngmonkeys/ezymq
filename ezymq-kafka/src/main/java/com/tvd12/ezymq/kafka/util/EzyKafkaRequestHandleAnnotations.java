package com.tvd12.ezymq.kafka.util;

import com.tvd12.ezymq.kafka.annotation.EzyKafkaRequestHandle;

public final class EzyKafkaRequestHandleAnnotations {

	private EzyKafkaRequestHandleAnnotations() {}
	
	public static String getCommand(EzyKafkaRequestHandle annotation) {
		String cmd = annotation.value();
		if(cmd.isEmpty())
			cmd = annotation.cmd();
		return cmd;
	}
	
}
