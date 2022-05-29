package com.tvd12.ezymq.kafka;

import com.tvd12.ezymq.common.EzyMQProxyBuilder;
import com.tvd12.ezymq.common.setting.EzyMQSettings;
import com.tvd12.ezymq.kafka.codec.EzyKafkaBytesDataCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaRecordsHandler;
import com.tvd12.ezymq.kafka.setting.EzyKafkaSettings;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class EzyKafkaProxyBuilder extends EzyMQProxyBuilder<
    EzyKafkaSettings,
    EzyKafkaDataCodec,
    EzyKafkaProxy,
    EzyKafkaProxyBuilder
    > {

    protected Map<String, Map<String, Class>> messageTypesByTopic =
        new HashMap<>();

    @Override
    protected EzyMQSettings.Builder newSettingBuilder() {
        return new EzyKafkaSettings.Builder(this);
    }

    @Override
    public EzyKafkaSettings.Builder settingsBuilder() {
        return (EzyKafkaSettings.Builder) super.settingsBuilder();
    }

    public EzyKafkaProxyBuilder mapMessageType(String topic, Class messageType) {
        return mapMessageType(topic, "", messageType);
    }

    public EzyKafkaProxyBuilder mapMessageType(String topic, String cmd, Class messageType) {
        this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
            .put(cmd, messageType);
        return this;
    }

    public EzyKafkaProxyBuilder mapMessageTypes(String topic, Map<String, Class> messageTypeByCommand) {
        this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
            .putAll(messageTypeByCommand);
        return this;
    }

    public EzyKafkaProxyBuilder mapMessageTypes(Map<String, Map<String, Class>> messageTypesByTopic) {
        this.messageTypesByTopic.putAll(messageTypesByTopic);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void decorateSettingBuilder(
        EzyMQSettings.Builder settingsBuilder
    ) {
        ((EzyKafkaSettings.Builder) settingsBuilder)
            .mapMessageTypes(messageTypesByTopic)
            .addConsumerInterceptors(
                beanContext.getSingletons(
                    getRequestInterceptorAnnotation()
                )
            )
            .addConsumerMessageHandlers(
                beanContext.getSingletons(
                    getRequestHandlerAnnotation()
                )
            );
    }

    @Override
    public Class<?> getRequestInterceptorAnnotation() {
        return EzyKafkaMessageInterceptor.class;
    }

    @Override
    public Class<?> getRequestHandlerAnnotation() {
        return EzyKafkaRecordsHandler.class;
    }

    @Override
    protected EzyKafkaProxy newProxy() {
        return new EzyKafkaProxy(settings, dataCodec, entityCodec);
    }

    @Override
    protected EzyKafkaDataCodec newDataCodec() {
        return EzyKafkaBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .textMessageDeserializer(textMessageDeserializer)
            .mapMessageTypes(settings.getMessageTypesByTopic())
            .build();
    }
}
