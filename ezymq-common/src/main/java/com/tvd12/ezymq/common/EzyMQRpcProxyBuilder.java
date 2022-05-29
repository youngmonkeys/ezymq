package com.tvd12.ezymq.common;

import com.tvd12.ezymq.common.codec.EzyMQBytesDataCodec;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.setting.EzyMQRpcSettings;
import com.tvd12.ezymq.common.setting.EzyMQSettings;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyMQRpcProxyBuilder<
    S extends EzyMQRpcSettings,
    P extends EzyMQProxy<S, EzyMQDataCodec>,
    PB extends EzyMQRpcProxyBuilder<S, P, PB>
    >
    extends EzyMQProxyBuilder<S, EzyMQDataCodec, P, PB> {

    protected final Map<String, Class> requestTypeByCommand =
        new HashMap<>();

    @Override
    public EzyMQRpcSettings.Builder settingsBuilder() {
        return (EzyMQRpcSettings.Builder) super.settingsBuilder();
    }

    @Override
    protected abstract EzyMQRpcSettings.Builder newSettingBuilder();

    public PB mapRequestType(String cmd, Class<?> type) {
        this.requestTypeByCommand.put(cmd, type);
        return (PB) this;
    }

    public PB mapRequestTypes(Map<String, Class> requestTypes) {
        this.requestTypeByCommand.putAll(requestTypes);
        return (PB) this;
    }

    @Override
    protected final void decorateSettingBuilder(
        EzyMQSettings.Builder settingsBuilder
    ) {
        EzyMQRpcSettings.Builder builder =
            (EzyMQRpcSettings.Builder) settingsBuilder;
        builder
            .mapRequestTypes(requestTypeByCommand)
            .addRequestInterceptors(
                beanContext.getSingletons(
                    getRequestInterceptorAnnotation()
                )
            )
            .addRequestHandlers(
                beanContext.getSingletons(
                    getRequestHandlerAnnotation()
                )
            );
        decorateSettingBuilder(builder);
    }

    protected void decorateSettingBuilder(
        EzyMQRpcSettings.Builder settingsBuilder
    ) {}

    @Override
    protected EzyMQDataCodec newDataCodec() {
        return EzyMQBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestTypes(requestTypeByCommand)
            .build();
    }
}
