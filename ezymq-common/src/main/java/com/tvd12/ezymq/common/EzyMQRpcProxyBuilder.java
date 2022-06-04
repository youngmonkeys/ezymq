package com.tvd12.ezymq.common;

import com.tvd12.ezymq.common.codec.EzyMQBytesDataCodec;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import com.tvd12.ezymq.common.setting.EzyMQSettings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyMQRpcProxyBuilder<
    S extends EzyMQRpcSettings,
    P extends EzyMQProxy<S, EzyMQDataCodec>,
    PB extends EzyMQRpcProxyBuilder<S, P, PB>
    >
    extends EzyMQProxyBuilder<S, EzyMQDataCodec, P, PB> {

    protected final Map<String, Class> requestTypeByCommand =
        new HashMap<>();
    protected final Map<String, Map<String, Class>> messageTypeMapByTopic =
        new HashMap<>();

    @Override
    public EzyMQRpcSettings.Builder settingsBuilder() {
        return (EzyMQRpcSettings.Builder) super.settingsBuilder();
    }

    @Override
    protected abstract EzyMQRpcSettings.Builder newSettingBuilder();

    public PB mapRequestType(String cmd, Class<?> requestType) {
        this.requestTypeByCommand.put(cmd, requestType);
        return (PB) this;
    }

    public PB mapRequestTypes(Map<String, Class> requestTypes) {
        this.requestTypeByCommand.putAll(requestTypes);
        return (PB) this;
    }

    public PB mapTopicMessageType(
        String topic,
        String cmd,
        Class<?> messageType
    ) {
        this.messageTypeMapByTopic.computeIfAbsent(
            topic,
            k -> new HashMap<>()
        ).put(cmd, messageType);
        return (PB) this;
    }

    public PB mapTopicMessageTypes(
        String topic,
        Map<String, Class<?>> messageTypes
    ) {
        this.messageTypeMapByTopic.computeIfAbsent(
            topic,
            k -> new HashMap<>()
        ).putAll(messageTypes);
        return (PB) this;
    }

    @Override
    protected Set<Class<?>> getBeanAnnotationClasses() {
        Set<Class<?>> annotationClasses = new HashSet<>(
            super.getBeanAnnotationClasses()
        );
        annotationClasses.add(getMessageConsumerAnnotationClass());
        return annotationClasses;
    }

    protected abstract Class<?> getMessageConsumerAnnotationClass();

    @Override
    protected void decorateSettingBuilder(
        EzyMQSettings.Builder settingsBuilder
    ) {
        EzyMQRpcSettings.Builder builder =
            (EzyMQRpcSettings.Builder) settingsBuilder;
        builder
            .mapRequestTypes(requestTypeByCommand)
            .mapTopicMessageTypes(messageTypeMapByTopic)
            .addRequestInterceptors(
                beanContext.getSingletons(
                    getRequestInterceptorAnnotationClass()
                )
            )
            .addRequestHandlers(
                beanContext.getSingletons(
                    getRequestHandlerAnnotationClass()
                )
            )
            .addMessageConsumers(
                beanContext.getSingletons(
                    getMessageConsumerAnnotationClass()
                )
            );
    }

    @Override
    protected EzyMQDataCodec newDataCodec() {
        return EzyMQBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestTypes(settings.getRequestTypeByCommand())
            .mapTopicMessageTypes(settings.getMessageTypeMapByTopic())
            .build();
    }
}
