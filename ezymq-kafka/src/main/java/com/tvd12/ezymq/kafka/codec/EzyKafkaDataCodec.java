package com.tvd12.ezymq.kafka.codec;

import com.tvd12.ezymq.common.codec.EzyMQDataSerializer;

public interface EzyKafkaDataCodec extends
    EzyMQDataSerializer,
    EzyKafkaDataDeserializer {}
