package com.tvd12.ezymq.activemq.setting;

import java.util.Collections;
import java.util.Map;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;

import lombok.Getter;

@Getter
public class EzyActiveSettings {

	protected final EzyEntityCodec entityCodec;
	protected final EzyActiveDataCodec dataCodec;
	protected final Map<String, EzyActiveTopicSetting> topicSettings;
	protected final Map<String, EzyActiveRpcCallerSetting> rpcCallerSettings;
	protected final Map<String, EzyActiveRpcHandlerSetting> rpcHandlerSettings;
	
	public EzyActiveSettings(
			EzyEntityCodec entityCodec,
			EzyActiveDataCodec dataCodec,
			Map<String, EzyActiveTopicSetting> topicSettings,
			Map<String, EzyActiveRpcCallerSetting> rpcCallerSettings,
			Map<String, EzyActiveRpcHandlerSetting> rpcHandlerSettings) {
		this.dataCodec = dataCodec;
		this.entityCodec = entityCodec;
		this.topicSettings = Collections.unmodifiableMap(topicSettings);
		this.rpcCallerSettings = Collections.unmodifiableMap(rpcCallerSettings);
		this.rpcHandlerSettings = Collections.unmodifiableMap(rpcHandlerSettings);
	}
	
}
