package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.binding.EzyBindingContext;
import com.tvd12.ezyfox.binding.EzyBindingContextBuilder;
import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.binding.impl.EzySimpleBindingContext;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.codec.*;
import com.tvd12.ezyfox.reflect.EzyReflection;
import com.tvd12.ezyfox.reflect.EzyReflectionProxy;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitBytesDataCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitBytesEntityCodec;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactoryBuilder;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;

import java.util.*;

@SuppressWarnings("rawtypes")
public class EzyRabbitMQProxyBuilder implements EzyBuilder<EzyRabbitMQProxy> {

    protected EzyRabbitSettings settings;
    protected Set<String> packagesToScan;
    protected EzyMarshaller marshaller;
    protected EzyUnmarshaller unmarshaller;
    protected EzyEntityCodec entityCodec;
    protected EzyRabbitDataCodec dataCodec;
    protected Map<String, Class> requestTypes;
    protected EzyBindingContext bindingContext;
    protected ConnectionFactory connectionFactory;
    protected EzyMessageSerializer messageSerializer;
    protected EzyMessageDeserializer messageDeserializer;
    protected List<EzyReflection> reflectionsToScan;
    protected EzyRabbitSettings.Builder settingsBuilder;

    public EzyRabbitMQProxyBuilder() {
        this.requestTypes = new HashMap<>();
        this.packagesToScan = new HashSet<>();
        this.reflectionsToScan = new ArrayList<>();
    }

    public EzyRabbitMQProxyBuilder scan(String packageName) {
        this.packagesToScan.add(packageName);
        return this;
    }

    public EzyRabbitMQProxyBuilder scan(String... packageNames) {
        return scan(Arrays.asList(packageNames));
    }

    public EzyRabbitMQProxyBuilder scan(Iterable<String> packageNames) {
        for (String packageName : packageNames) {
            scan(packageName);
        }
        return this;
    }

    public EzyRabbitMQProxyBuilder scan(EzyReflection reflection) {
        this.reflectionsToScan.add(reflection);
        return this;
    }

    public EzyRabbitSettings.Builder settingsBuilder() {
        if (settingsBuilder == null) {
            settingsBuilder = new EzyRabbitSettings.Builder(this);
        }
        return settingsBuilder;
    }

    public EzyRabbitMQProxyBuilder settings(EzyRabbitSettings settings) {
        this.settings = settings;
        return this;
    }

    public EzyRabbitMQProxyBuilder marshaller(EzyMarshaller marshaller) {
        this.marshaller = marshaller;
        return this;
    }

    public EzyRabbitMQProxyBuilder unmarshaller(EzyUnmarshaller unmarshaller) {
        this.unmarshaller = unmarshaller;
        return this;
    }

    public EzyRabbitMQProxyBuilder entityCodec(EzyEntityCodec entityCodec) {
        this.entityCodec = entityCodec;
        return this;
    }

    public EzyRabbitMQProxyBuilder dataCodec(EzyRabbitDataCodec dataCodec) {
        this.dataCodec = dataCodec;
        return this;
    }

    public EzyRabbitMQProxyBuilder bindingContext(EzyBindingContext bindingContext) {
        this.bindingContext = bindingContext;
        return this;
    }

    public EzyRabbitMQProxyBuilder connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    public EzyRabbitMQProxyBuilder messageSerializer(EzyMessageSerializer messageSerializer) {
        this.messageSerializer = messageSerializer;
        return this;
    }

    public EzyRabbitMQProxyBuilder messageDeserializer(EzyMessageDeserializer messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
        return this;
    }

    public EzyRabbitMQProxyBuilder mapRequestType(String cmd, Class<?> type) {
        this.requestTypes.put(cmd, type);
        return this;
    }

    public EzyRabbitMQProxyBuilder mapRequestTypes(Map<String, Class> requestTypes) {
        this.requestTypes.putAll(requestTypes);
        return this;
    }

    @Override
    public EzyRabbitMQProxy build() {
        if (settingsBuilder != null) {
            settings = settingsBuilder.build();
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
            connectionFactory = new EzyRabbitConnectionFactoryBuilder().build();
        }
        return new EzyRabbitMQProxy(entityCodec, dataCodec, settings, connectionFactory);
    }

    protected EzyEntityCodec newEntityCodec() {
        return EzyRabbitBytesEntityCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .build();
    }

    protected EzyRabbitDataCodec newDataCodec() {
        return EzyRabbitBytesDataCodec.builder()
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

    private EzyBindingContext newBindingContext() {
        if (packagesToScan.size() > 0) {
            reflectionsToScan.add(new EzyReflectionProxy(packagesToScan));
        }
        EzyBindingContextBuilder builder = EzySimpleBindingContext.builder();
        for (EzyReflection reflection : reflectionsToScan) {
            builder.addAllClasses(reflection);
        }
        return builder.build();
    }
}
