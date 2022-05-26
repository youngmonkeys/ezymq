package com.tvd12.ezymq.kafka.manager;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaClient;
import com.tvd12.ezymq.kafka.setting.EzyKafkaProducerSetting;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public class EzyKafkaProducerManager
    extends EzyKafkaAbstractManager implements EzyCloseable {

    protected final EzyEntityCodec entityCodec;
    @Getter
    protected final Map<String, EzyKafkaProducer> producers;
    protected final Map<String, EzyKafkaProducerSetting> producerSettings;

    public EzyKafkaProducerManager(
        EzyEntityCodec entityCodec,
        Map<String, EzyKafkaProducerSetting> producerSettings
    ) {
        this.entityCodec = entityCodec;
        this.producerSettings = producerSettings;
        this.producers = createProducers();
    }

    public EzyKafkaProducer getProducer(String name) {
        EzyKafkaProducer producer = producers.get(name);
        if (producer == null) {
            throw new IllegalArgumentException("has no producer with name: " + name);
        }
        return producer;
    }

    protected Map<String, EzyKafkaProducer> createProducers() {
        Map<String, EzyKafkaProducer> map = new HashMap<>();
        for (String name : producerSettings.keySet()) {
            EzyKafkaProducerSetting setting = producerSettings.get(name);
            map.put(name, createProducer(name, setting));
        }
        return map;
    }

    protected EzyKafkaProducer createProducer(
        String name,
        EzyKafkaProducerSetting setting
    ) {
        try {
            return createProducer(setting);
        } catch (Exception e) {
            throw new IllegalStateException("create producer: " + name + " error", e);
        }
    }

    protected EzyKafkaProducer createProducer(
        EzyKafkaProducerSetting setting
    ) {
        EzyKafkaClient client = EzyKafkaClient.builder()
            .topic(setting.getTopic())
            .producer(setting.getProducer())
            .properties(setting.getProperties())
            .build();
        return EzyKafkaProducer.builder()
            .entityCodec(entityCodec)
            .client(client).build();
    }

    @Override
    public void close() {
        for (EzyKafkaProducer producer : producers.values()) {
            producer.close();
        }
    }
}
