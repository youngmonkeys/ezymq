package com.tvd12.ezymq.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tvd12.ezyfox.bean.EzyBeanContext;
import com.tvd12.ezyfox.bean.EzyBeanContextBuilder;
import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyBindingContextBuilder;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.binding.codec.EzyBindingEntityCodec;
import com.tvd12.ezyfox.binding.impl.EzySimpleBindingContext;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.*;
import com.tvd12.ezyfox.entity.EzyData;
import com.tvd12.ezyfox.message.annotation.EzyMessage;
import com.tvd12.ezyfox.reflect.EzyReflection;
import com.tvd12.ezyfox.reflect.EzyReflectionProxy;
import com.tvd12.ezyfox.reflect.EzyTypes;
import com.tvd12.ezyfox.util.EzyPropertiesKeeper;
import com.tvd12.ezymq.common.setting.EzyMQSettings;

import java.lang.annotation.Annotation;
import java.util.*;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class EzyMQProxyBuilder<
    S extends EzyMQSettings,
    D,
    P extends EzyMQProxy<S, D>,
    PB extends EzyMQProxyBuilder<S, D, P, PB>
    >
    extends EzyPropertiesKeeper<PB>
    implements EzyBuilder<P> {

    protected S settings;
    protected D dataCodec;
    protected boolean scanAndAddAllBeans;
    protected Set<String> packagesToScan;
    protected EzyMarshaller marshaller;
    protected EzyUnmarshaller unmarshaller;
    protected EzyEntityCodec entityCodec;
    protected EzyBeanContext beanContext;
    protected EzyBindingContext bindingContext;
    protected EzyMessageSerializer messageSerializer;
    protected EzyMessageDeserializer messageDeserializer;
    protected EzyMessageDeserializer textMessageDeserializer;
    protected EzyMQSettings.Builder settingsBuilder;
    protected EzyBeanContextBuilder beanContextBuilder;
    protected Map<String, Map<String, Class>> messageTypesByTopic;

    public EzyMQProxyBuilder() {
        this.messageTypesByTopic = new HashMap<>();
        this.packagesToScan = new HashSet<>();
        this.beanContextBuilder = EzyBeanContext.builder();
    }

    public PB entryClass(Class<?> entryClass) {
        return scan(entryClass.getPackage().getName());
    }

    public PB scan(String packageName) {
        this.packagesToScan.add(packageName);
        return (PB) this;
    }

    public PB scan(String... packageNames) {
        return scan(Arrays.asList(packageNames));
    }

    public PB scan(Iterable<String> packageNames) {
        for (String packageName : packageNames) {
            scan(packageName);
        }
        return (PB) this;
    }

    public EzyMQSettings.Builder settingsBuilder() {
        if (settingsBuilder == null) {
            settingsBuilder = newSettingBuilder();
        }
        return settingsBuilder;
    }

    protected abstract EzyMQSettings.Builder newSettingBuilder();

    public EzyMQProxyBuilder settings(S settings) {
        this.settings = settings;
        return this;
    }

    public EzyMQProxyBuilder marshaller(EzyMarshaller marshaller) {
        this.marshaller = marshaller;
        return this;
    }

    public EzyMQProxyBuilder unmarshaller(EzyUnmarshaller unmarshaller) {
        this.unmarshaller = unmarshaller;
        return this;
    }

    public EzyMQProxyBuilder entityCodec(EzyEntityCodec entityCodec) {
        this.entityCodec = entityCodec;
        return this;
    }

    public EzyMQProxyBuilder addSingleton(Object singleton) {
        this.beanContextBuilder.addSingleton(singleton);
        return this;
    }

    public EzyMQProxyBuilder addSingleton(String name, Object singleton) {
        this.beanContextBuilder.addSingleton(name, singleton);
        return this;
    }

    public EzyMQProxyBuilder addSingletons(List singletons) {
        singletons.forEach(beanContextBuilder::addSingleton);
        return this;
    }

    public EzyMQProxyBuilder addSingletons(Map singletons) {
        beanContextBuilder.addSingletons(singletons);
        return this;
    }

    public EzyMQProxyBuilder beanContext(EzyBeanContext beanContext) {
        this.beanContextBuilder.addSingletonsByKey(beanContext.getSingletonMapByKey());
        return this;
    }

    public EzyMQProxyBuilder bindingContext(EzyBindingContext bindingContext) {
        this.bindingContext = bindingContext;
        return this;
    }

    public EzyMQProxyBuilder scanAndAddAllBeans(boolean scanAndAddAllBeans) {
        this.scanAndAddAllBeans = scanAndAddAllBeans;
        return this;
    }

    public EzyMQProxyBuilder messageSerializer(EzyMessageSerializer messageSerializer) {
        this.messageSerializer = messageSerializer;
        return this;
    }

    public EzyMQProxyBuilder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
        return this;
    }

    public EzyMQProxyBuilder textMessageDeserializer(EzyMessageDeserializer textMessageDeserializer) {
        this.textMessageDeserializer = textMessageDeserializer;
        return this;
    }

    public EzyMQProxyBuilder mapMessageType(String topic, Class messageType) {
        return mapMessageType(topic, "", messageType);
    }

    public EzyMQProxyBuilder mapMessageType(String topic, String cmd, Class messageType) {
        this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
            .put(cmd, messageType);
        return this;
    }

    public EzyMQProxyBuilder mapMessageTypes(String topic, Map<String, Class> messageTypeByCommand) {
        this.messageTypesByTopic.computeIfAbsent(topic, k -> new HashMap<>())
            .putAll(messageTypeByCommand);
        return this;
    }

    public EzyMQProxyBuilder mapMessageTypes(Map<String, Map<String, Class>> messageTypesByTopic) {
        this.messageTypesByTopic.putAll(messageTypesByTopic);
        return this;
    }

    @Override
    public P build() {
        beanContext = newBeanContext();
        if (settings == null) {
            if (settingsBuilder == null) {
                settingsBuilder = newSettingBuilder();
            }
            decorateSettingBuilder(settingsBuilder);
            settings = (S) settingsBuilder.build();
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
        return newProxy();
    }

    protected abstract void decorateSettingBuilder(
        EzyMQSettings.Builder settingsBuilder
    );

    protected abstract D newDataCodec();

    protected abstract P newProxy();

    protected EzyEntityCodec newEntityCodec() {
        return EzyBindingEntityCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
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

    private EzyBeanContext newBeanContext() {
        if (packagesToScan.size() > 0) {
            EzyReflection reflection = new EzyReflectionProxy(packagesToScan);
            for (Class beanAnnotationClass : getBeanAnnotationClasses()) {
                beanContextBuilder.addSingletonClasses(
                    reflection.getAnnotatedClasses(beanAnnotationClass)
                );
            }
            if (scanAndAddAllBeans) {
                beanContextBuilder.scan(packagesToScan);
            }
        }
        return beanContextBuilder.build();
    }

    private Class<? extends Annotation>[] getBeanAnnotationClasses() {
        return new Class[] {
            getRequestInterceptorAnnotation(),
            getRequestHandlerAnnotation()
        };
    }

    public abstract Class<?> getRequestInterceptorAnnotation();

    public abstract Class<?> getRequestHandlerAnnotation();

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
