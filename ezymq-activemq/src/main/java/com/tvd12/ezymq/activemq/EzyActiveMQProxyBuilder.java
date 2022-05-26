package com.tvd12.ezymq.activemq;

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
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.activemq.annotation.EzyActiveHandler;
import com.tvd12.ezymq.activemq.annotation.EzyActiveInterceptor;
import com.tvd12.ezymq.activemq.codec.EzyActiveBytesDataCodec;
import com.tvd12.ezymq.activemq.codec.EzyActiveBytesEntityCodec;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactoryBuilder;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;

import javax.jms.ConnectionFactory;
import java.util.*;

@SuppressWarnings("rawtypes")
public class EzyActiveMQProxyBuilder
    extends EzyLoggable
    implements EzyBuilder<EzyActiveMQProxy> {

    protected boolean scanAndAddAllBeans;
    protected EzyActiveSettings settings;
    protected Set<String> packagesToScan;
    protected EzyMarshaller marshaller;
    protected EzyUnmarshaller unmarshaller;
    protected EzyEntityCodec entityCodec;
    protected EzyActiveDataCodec dataCodec;
    protected Map<String, Class> requestTypes;
    protected EzyBeanContext beanContext;
    protected EzyBindingContext bindingContext;
    protected ConnectionFactory connectionFactory;
    protected EzyMessageSerializer messageSerializer;
    protected EzyMessageDeserializer messageDeserializer;
    protected List<EzyReflection> reflectionsToScan;
    protected EzyActiveSettings.Builder settingsBuilder;
    protected EzyBeanContextBuilder beanContextBuilder;

    public EzyActiveMQProxyBuilder() {
        this.requestTypes = new HashMap<>();
        this.packagesToScan = new HashSet<>();
        this.reflectionsToScan = new ArrayList<>();
    }

    public EzyActiveMQProxyBuilder scan(String packageName) {
        this.packagesToScan.add(packageName);
        return this;
    }

    public EzyActiveMQProxyBuilder scan(String... packageNames) {
        return scan(Arrays.asList(packageNames));
    }

    public EzyActiveMQProxyBuilder scan(Iterable<String> packageNames) {
        for (String packageName : packageNames) {
            scan(packageName);
        }
        return this;
    }

    public EzyActiveMQProxyBuilder scan(EzyReflection reflection) {
        this.reflectionsToScan.add(reflection);
        return this;
    }

    public EzyActiveSettings.Builder settingsBuilder() {
        if (settingsBuilder == null) {
            settingsBuilder = new EzyActiveSettings.Builder(this);
        }
        return settingsBuilder;
    }

    public EzyActiveMQProxyBuilder settings(EzyActiveSettings settings) {
        this.settings = settings;
        return this;
    }

    public EzyActiveMQProxyBuilder marshaller(EzyMarshaller marshaller) {
        this.marshaller = marshaller;
        return this;
    }

    public EzyActiveMQProxyBuilder unmarshaller(EzyUnmarshaller unmarshaller) {
        this.unmarshaller = unmarshaller;
        return this;
    }

    public EzyActiveMQProxyBuilder entityCodec(EzyEntityCodec entityCodec) {
        this.entityCodec = entityCodec;
        return this;
    }

    public EzyActiveMQProxyBuilder dataCodec(EzyActiveDataCodec dataCodec) {
        this.dataCodec = dataCodec;
        return this;
    }

    public EzyActiveMQProxyBuilder addSingleton(Object singleton) {
        this.beanContextBuilder.addSingleton(singleton);
        return this;
    }

    public EzyActiveMQProxyBuilder addSingleton(String name, Object singleton) {
        this.beanContextBuilder.addSingleton(name, singleton);
        return this;
    }

    @SuppressWarnings("unchecked")
    public EzyActiveMQProxyBuilder addSingletons(List singletons) {
        singletons.forEach(beanContextBuilder::addSingleton);
        return this;
    }

    @SuppressWarnings("unchecked")
    public EzyActiveMQProxyBuilder addSingletons(Map singletons) {
        beanContextBuilder.addSingletons(singletons);
        return this;
    }

    public EzyActiveMQProxyBuilder beanContext(EzyBeanContext beanContext) {
        this.beanContextBuilder.addSingletonsByKey(beanContext.getSingletonMapByKey());
        return this;
    }

    public EzyActiveMQProxyBuilder bindingContext(EzyBindingContext bindingContext) {
        this.bindingContext = bindingContext;
        return this;
    }

    public EzyActiveMQProxyBuilder scanAndAddAllBeans(boolean scanAndAddAllBeans) {
        this.scanAndAddAllBeans = scanAndAddAllBeans;
        return this;
    }

    public EzyActiveMQProxyBuilder connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    public EzyActiveMQProxyBuilder messageSerializer(EzyMessageSerializer messageSerializer) {
        this.messageSerializer = messageSerializer;
        return this;
    }

    public EzyActiveMQProxyBuilder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
        return this;
    }

    public EzyActiveMQProxyBuilder mapRequestType(String cmd, Class<?> type) {
        this.requestTypes.put(cmd, type);
        return this;
    }

    public EzyActiveMQProxyBuilder mapRequestTypes(Map<String, Class> requestTypes) {
        this.requestTypes.putAll(requestTypes);
        return this;
    }

    @Override
    public EzyActiveMQProxy build() {
        beanContext = newBeanContext();

        if (settings == null) {
            if (settingsBuilder == null) {
                settingsBuilder = EzyActiveSettings.builder();
            }
            settingsBuilder.mapRequestTypes(requestTypes);
            List interceptors = beanContext.getSingletons(EzyActiveInterceptor.class);
            List dataHandlers = beanContext.getSingletons(EzyActiveHandler.class);
            settings = settingsBuilder
                .requestInterceptors(interceptors)
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
        if (dataCodec == null) {
            dataCodec = newDataCodec();
        }
        if (entityCodec == null) {
            entityCodec = newEntityCodec();
        }
        if (connectionFactory == null) {
            connectionFactory = new EzyActiveConnectionFactoryBuilder().build();
        }
        return new EzyActiveMQProxy(entityCodec, dataCodec, settings, connectionFactory);
    }

    protected EzyEntityCodec newEntityCodec() {
        return EzyActiveBytesEntityCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .build();
    }

    protected EzyActiveDataCodec newDataCodec() {
        return EzyActiveBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestTypes(requestTypes)
            .build();
    }

    protected EzyMessageSerializer newMessageSerializer() {
        return new MsgPackSimpleSerializer();
    }

    protected EzyMessageDeserializer newMessageDeserializer() {
        return new MsgPackSimpleDeserializer();
    }

    @SuppressWarnings("unchecked")
    private EzyBeanContext newBeanContext() {
        if (packagesToScan.size() > 0) {
            EzyReflection reflection = new EzyReflectionProxy(packagesToScan);
            beanContextBuilder.addSingletonClasses((Set) reflection.getAnnotatedClasses(EzyActiveHandler.class));
            beanContextBuilder.addSingletonClasses((Set) reflection.getAnnotatedClasses(EzyActiveInterceptor.class));

            if (scanAndAddAllBeans) {
                beanContextBuilder.scan(packagesToScan);
            }
        }
        return beanContextBuilder.build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
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
