package com.tvd12.ezymq.mosquitto.manager;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcProducer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcClient;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallbackProxy;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoRpcProducerSetting;

public class EzyMosquittoRpcProducerManager
    extends EzyMosquittoAbstractManager
    implements EzyCloseable {

    protected final EzyEntityCodec entityCodec;
    protected final Map<String, EzyMosquittoRpcProducer> rpProducers;
    protected final Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings;

    public EzyMosquittoRpcProducerManager(
        MqttClient mqttClient,
        EzyMqttCallbackProxy mqttCallbackProxy,
        EzyEntityCodec entityCodec,
        Map<String, EzyMosquittoRpcProducerSetting> rpcProducerSettings
    ) {
        super(mqttClient);
        this.entityCodec = entityCodec;
        this.rpcProducerSettings = rpcProducerSettings;
        this.rpProducers = createRpcProducers(mqttCallbackProxy);
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

    protected Map<String, EzyMosquittoRpcProducer> createRpcProducers(
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        Map<String, EzyMosquittoRpcProducer> map = new HashMap<>();
        for (String name : rpcProducerSettings.keySet()) {
            EzyMosquittoRpcProducerSetting setting = rpcProducerSettings.get(name);
            map.put(
                name,
                createRpcProducer(
                    name,
                    setting,
                    mqttCallbackProxy
                )
            );
        }
        return map;
    }

    protected EzyMosquittoRpcProducer createRpcProducer(
        String name,
        EzyMosquittoRpcProducerSetting setting,
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        try {
            return createRpcProducer(
                setting,
                mqttCallbackProxy
            );
        } catch (Exception e) {
            throw new IllegalStateException(
                "create rpc producer: " + name + " error",
                e
            );
        }
    }

    protected EzyMosquittoRpcProducer createRpcProducer(
        EzyMosquittoRpcProducerSetting setting,
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        EzyMosquittoRpcClient client = EzyMosquittoRpcClient
            .builder()
            .topic(setting.getTopic())
            .mqttClient(mqttClient)
            .mqttCallbackProxy(mqttCallbackProxy)
            .capacity(setting.getCapacity())
            .defaultTimeout(setting.getDefaultTimeout())
            .messageIdFactory(setting.getMessageIdFactory())
            .unconsumedResponseConsumer(setting.getUnconsumedResponseConsumer())
            .build();
        return EzyMosquittoRpcProducer
            .builder()
            .entityCodec(entityCodec)
            .client(client).build();
    }

    public void close() {
        for (EzyMosquittoRpcProducer producer : rpProducers.values()) {
            producer.close();
        }
    }
}
