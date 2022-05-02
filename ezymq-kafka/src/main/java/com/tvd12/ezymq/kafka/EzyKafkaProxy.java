package com.tvd12.ezymq.kafka;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.manager.EzyKafkaConsumerManager;
import com.tvd12.ezymq.kafka.manager.EzyKafkaProducerManager;
import com.tvd12.ezymq.kafka.setting.EzyKafkaSettings;

import java.util.Map;

public class EzyKafkaProxy implements EzyCloseable {

    protected final EzyKafkaSettings settings;
    protected final EzyEntityCodec entityCodec;
    protected final EzyKafkaDataCodec dataCodec;
    protected final EzyKafkaProducerManager producerManager;
    protected final EzyKafkaConsumerManager consumerManager;

    public EzyKafkaProxy(
        EzyEntityCodec entityCodec,
        EzyKafkaDataCodec dataCodec,
        EzyKafkaSettings settings
    ) {
        this.settings = settings;
        this.dataCodec = dataCodec;
        this.entityCodec = entityCodec;
        this.producerManager = newProducerManager();
        this.consumerManager = newConsumerManager();

    }

    public static EzyKafkaProxyBuilder builder() {
        return new EzyKafkaProxyBuilder();
    }

    public EzyKafkaProducer getProducer(String name) {
        return producerManager.getProducer(name);
    }

    public EzyKafkaConsumer getConsumer(String name) {
        return consumerManager.getConsumer(name);
    }

    public void startConsumers() throws Exception {
        consumerManager.startConsumers();
    }

    public Map<String, EzyKafkaConsumer> getConsumers() {
        return consumerManager.getConsumers();
    }

    @Override
    public void close() {
        producerManager.close();
        consumerManager.close();
    }

    protected EzyKafkaProducerManager newProducerManager() {
        return new EzyKafkaProducerManager(
            entityCodec,
            settings.getProducerSettings()
        );
    }

    protected EzyKafkaConsumerManager newConsumerManager() {
        return new EzyKafkaConsumerManager(
            dataCodec,
            settings.getConsumerSettings()
        );
    }
}
