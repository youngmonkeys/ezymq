package com.tvd12.ezymq.mosquitto.manager;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcProducer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcClient;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoRpcProducerSetting;

import java.util.HashMap;
import java.util.Map;

public class EzyMosquittoRpcProducerManager
    extends EzyMosquittoAbstractManager
    implements EzyCloseable {

    protected final EzyEntityCodec entityCodec;
    protected final Map<String, EzyMosquittoRpcProducer> rpProducers;
    protected final Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings;

    public EzyMosquittoRpcProducerManager(
        EzyMqttClientProxy mqttClient,
        EzyEntityCodec entityCodec,
        Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings
    ) {
        super(mqttClient);
        this.entityCodec = entityCodec;
        this.rpcProducerSettings = rpcProducerSettings;
        this.rpProducers = createRpcProducers();
    }

    public EzyMosquittoRpcProducer getRpcProducer(String name) {
        EzyMosquittoRpcProducer producer = rpProducers.get(name);
        if (producer == null) {
            throw new IllegalArgumentException(
                "has no rpc producer with name: " + name
            );
        }
        return producer;
    }

    protected Map<String, EzyMosquittoRpcProducer> createRpcProducers() {
        Map<String, EzyMosquittoRpcProducer> map = new HashMap<>();
        for (String name : rpcProducerSettings.keySet()) {
            EzyMosquittoRpcProducerSetting setting = rpcProducerSettings.get(name);
            map.put(
                name,
                createRpcProducer(name, setting)
            );
        }
        return map;
    }

    protected EzyMosquittoRpcProducer createRpcProducer(
        String name,
        EzyMosquittoRpcProducerSetting setting
    ) {
        try {
            return createRpcProducer(setting);
        } catch (Exception e) {
            throw new IllegalStateException(
                "create rpc producer: " + name + " error",
                e
            );
        }
    }

    protected EzyMosquittoRpcProducer createRpcProducer(
        EzyMosquittoRpcProducerSetting setting
    ) throws Exception {
        String topic = setting.getTopic();
        EzyMosquittoRpcClient client = EzyMosquittoRpcClient
            .builder()
            .topic(topic)
            .mqttClient(mqttClient)
            .capacity(setting.getCapacity())
            .defaultTimeout(setting.getDefaultTimeout())
            .messageIdFactory(setting.getMessageIdFactory())
            .unconsumedResponseConsumer(setting.getUnconsumedResponseConsumer())
            .build();
        EzyMosquittoRpcProducer producer = EzyMosquittoRpcProducer
            .builder()
            .entityCodec(entityCodec)
            .client(client)
            .build();
        mqttClient.subscribe(topic);
        return producer;
    }

    public void close() {
        for (EzyMosquittoRpcProducer producer : rpProducers.values()) {
            producer.close();
        }
    }
}
