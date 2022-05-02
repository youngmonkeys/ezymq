package com.tvd12.ezymq.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tvd12.ezyfox.bean.EzyBeanContext;
import com.tvd12.ezyfox.bean.EzyBeanContextBuilder;
import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyBindingContextBuilder;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.binding.impl.EzySimpleBindingContext;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.*;
import com.tvd12.ezyfox.entity.EzyData;
import com.tvd12.ezyfox.message.annotation.EzyMessage;
import com.tvd12.ezyfox.reflect.EzyReflection;
import com.tvd12.ezyfox.reflect.EzyReflectionProxy;
import com.tvd12.ezyfox.reflect.EzyTypes;
import com.tvd12.ezyfox.util.EzyPropertiesKeeper;
import com.tvd12.ezymq.kafka.annotation.EzyKafkaHandler;
import com.tvd12.ezymq.kafka.annotation.EzyKafkaInterceptor;
import com.tvd12.ezymq.kafka.codec.EzyKafkaBytesDataCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaBytesEntityCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.setting.EzyKafkaSettings;

import java.util.*;

@SuppressWarnings("rawtypes")
public class EzyKafkaProxyBuilder
    extends EzyPropertiesKeeper<EzyKafkaProxyBuilder>
    implements EzyBuilder<EzyKafkaProxy> {

    protected EzyKafkaSettings settings;
    protected Set<String> packagesToScan;
    protected EzyMarshaller marshaller;
    protected EzyUnmarshaller unmarshaller;
    protected EzyEntityCodec entityCodec;
    protected EzyKafkaDataCodec dataCodec;
    protected EzyBeanContext beanContext;
    protected EzyBindingContext bindingContext;
    protected boolean ignoreUnknownComponents;
    protected EzyMessageSerializer messageSerializer;
    protected EzyMessageDeserializer messageDeserializer;
    protected EzyMessageDeserializer textMessageDeserializer;
    protected EzyKafkaSettings.Builder settingsBuilder;
    protected EzyBeanContextBuilder beanContextBuilder;
    protected Map<String, Map<String, Class>> messageTypesByTopic;

    public EzyKafkaProxyBuilder() {
        this.messageTypesByTopic = new HashMap<>();
        this.packagesToScan = new HashSet<>();
        this.beanContextBuilder = EzyBeanContext.builder();
    }

    public EzyKafkaProxyBuilder entryClass(Class<?> entryClass) {
        return scan(entryClass.getPackage().getName());
    }

    public EzyKafkaProxyBuilder scan(String packageName) {
        this.packagesToScan.add(packageName);
        return this;
    }

    public EzyKafkaProxyBuilder scan(String... packageNames) {
        return scan(Arrays.asList(packageNames));
    }

    public EzyKafkaProxyBuilder scan(Iterable<String> packageNames) {
        for (String packageName : packageNames) {
            scan(packageName);
        }
        return this;
    }

    public EzyKafkaSettings.Builder settingsBuilder() {
        if (settingsBuilder == null) {
            settingsBuilder = new EzyKafkaSettings.Builder(this);
        }
        return settingsBuilder;
    }

    public EzyKafkaProxyBuilder settings(EzyKafkaSettings settings) {
        this.settings = settings;
        return this;
    }

    public EzyKafkaProxyBuilder marshaller(EzyMarshaller marshaller) {
        this.marshaller = marshaller;
        return this;
    }

    public EzyKafkaProxyBuilder unmarshaller(EzyUnmarshaller unmarshaller) {
        this.unmarshaller = unmarshaller;
        return this;
    }

    public EzyKafkaProxyBuilder entityCodec(EzyEntityCodec entityCodec) {
        this.entityCodec = entityCodec;
        return this;
    }

    public EzyKafkaProxyBuilder dataCodec(EzyKafkaDataCodec dataCodec) {
        this.dataCodec = dataCodec;
        return this;
    }

    public EzyKafkaProxyBuilder addSingleton(Object singleton) {
        this.beanContextBuilder.addSingleton(singleton);
        return this;
    }

    public EzyKafkaProxyBuilder addSingleton(String name, Object singleton) {
        this.beanContextBuilder.addSingleton(name, singleton);
        return this;
    }

    public EzyKafkaProxyBuilder beanContext(EzyBeanContext beanContext) {
        this.beanContextBuilder.addSingletonsByKey(beanContext.getSingletonMapByKey());
        return this;
    }

    public EzyKafkaProxyBuilder bindingContext(EzyBindingContext bindingContext) {
        this.bindingContext = bindingContext;
        return this;
    }

    public EzyKafkaProxyBuilder ignoreUnknownComponents(boolean ignoreUnknownComponents) {
        this.ignoreUnknownComponents = ignoreUnknownComponents;
        return this;
    }

    public EzyKafkaProxyBuilder messageSerializer(EzyMessageSerializer messageSerializer) {
        this.messageSerializer = messageSerializer;
        return this;
    }

    public EzyKafkaProxyBuilder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
        return this;
    }

    public EzyKafkaProxyBuilder textMessageDeserializer(EzyMessageDeserializer textMessageDeserializer) {
        this.textMessageDeserializer = textMessageDeserializer;
        return this;
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
    public EzyKafkaProxy build() {
        beanContext = newBeanContext();

        if (settings == null) {
            if (settingsBuilder == null) {
                settingsBuilder = EzyKafkaSettings.builder();
            }
            settingsBuilder.properties((Map) beanContext.getProperties());
            settingsBuilder.mapMessageTypes(messageTypesByTopic);
            List interceptors = beanContext.getSingletons(EzyKafkaInterceptor.class);
            List dataHandlers = beanContext.getSingletons(EzyKafkaHandler.class);
            EzyKafkaMessageInterceptor interceptor = interceptors.isEmpty()
                ? null
                : (EzyKafkaMessageInterceptor) interceptors.get(0);
            settings = settingsBuilder
                .consumerInterceptor(interceptor)
                .consumerMessageHandlers(dataHandlers)
                .build();
        }
        if (bindingContext == null) {
            bindingContext = newBindingContext();
        }
        marshaller = bindingContext.newMarshaller();
        unmarshaller = bindingContext.newUnmarshaller();

        if (messageSerializer == null) {
            messageSerializer = newMessageSerializer();
        }
        if (messageDeserializer == null) {
            messageDeserializer = newMessageDeserializer();
        }
        if (textMessageDeserializer == null) {
            textMessageDeserializer = newTextMessageDeserializer();
        }
        if (dataCodec == null) {
            dataCodec = newDataCodec();
        }
        if (entityCodec == null) {
            entityCodec = newEntityCodec();
        }
        return new EzyKafkaProxy(entityCodec, dataCodec, settings);
    }

    protected EzyEntityCodec newEntityCodec() {
        return EzyKafkaBytesEntityCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .build();
    }

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

    protected EzyMessageSerializer newMessageSerializer() {
        return new MsgPackSimpleSerializer();
    }

    protected EzyMessageDeserializer newMessageDeserializer() {
        return new MsgPackSimpleDeserializer();
    }

    protected EzyMessageDeserializer newTextMessageDeserializer() {
        try {
            return new JacksonSimpleDeserializer(new ObjectMapper());
        } catch (Throwable e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private EzyBeanContext newBeanContext() {
        if (packagesToScan.size() > 0) {
            EzyReflection reflection = new EzyReflectionProxy(packagesToScan);
            beanContextBuilder.addSingletonClasses((Set) reflection.getAnnotatedClasses(EzyKafkaInterceptor.class));
            beanContextBuilder.addSingletonClasses((Set) reflection.getAnnotatedClasses(EzyKafkaHandler.class));

            if (ignoreUnknownComponents) {
                beanContextBuilder.scan(packagesToScan);
            }
        }
        return beanContextBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private EzyBindingContext newBindingContext() {
        EzyBindingContextBuilder builder = EzySimpleBindingContext.builder();
        for (Class messageType : settings.getMessageTypeList()) {
            if (EzyTypes.ALL_TYPES.contains(messageType)
                || EzyData.class.isAssignableFrom(messageType)
            ) {
                builder.addClasses(messageType);
            }
        }
        try {
            builder.build();
        } catch (Throwable e) {
            builder = EzySimpleBindingContext.builder();
            logger.debug("can not create biding context, try again", e);
        }
        if (packagesToScan.size() > 0) {
            EzyReflection reflection = new EzyReflectionProxy(packagesToScan);
            builder.addClasses((Set) reflection.getAnnotatedClasses(EzyMessage.class));
            builder.addAllClasses(reflection);
        }
        return builder.build();
    }
}
