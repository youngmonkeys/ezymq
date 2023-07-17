package com.tvd12.ezymq.mosquitto.manager;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcConsumer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcServer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallbackProxy;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoRpcConsumerSetting;

public class EzyMosquittoRpcConsumerManager extends EzyMosquittoAbstractManager {

    protected final EzyMQDataCodec dataCodec;
    protected final Map<String, EzyMosquittoRpcConsumer> rpcConsumers;
    protected final Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings;

    public EzyMosquittoRpcConsumerManager(
        MqttClient mqttClient,
        EzyMqttCallbackProxy mqttCallbackProxy,
        EzyMQDataCodec dataCodec,
        Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(mqttClient);
        this.dataCodec = dataCodec;
        this.rpcConsumerSettings = rpcConsumerSettings;
        this.rpcConsumers = createRpcConsumers(mqttCallbackProxy);
    }

    public EzyMosquittoRpcConsumer getRpcConsumer(String name) {
        EzyMosquittoRpcConsumer consumer = rpcConsumers.get(name);
        if (consumer == null) {
            throw new IllegalArgumentException(
                "has no rpc consumer with name: " + name
            );
        }
        return consumer;
    }

    protected Map<String, EzyMosquittoRpcConsumer> createRpcConsumers(
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        Map<String, EzyMosquittoRpcConsumer> map = new HashMap<>();
        for (String name : rpcConsumerSettings.keySet()) {
            EzyMosquittoRpcConsumerSetting setting = rpcConsumerSettings.get(name);
            map.put(name, createRpcConsumer(name, setting, mqttCallbackProxy));
        }
        return map;
    }

    protected EzyMosquittoRpcConsumer createRpcConsumer(
        String name,
        EzyMosquittoRpcConsumerSetting setting,
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        try {
            return createRpcConsumer(setting, mqttCallbackProxy);
        } catch (Exception e) {
            throw new IllegalStateException(
                "can't create rpc consumer: " + name,
                e
            );
        }
    }

    protected EzyMosquittoRpcConsumer createRpcConsumer(
        EzyMosquittoRpcConsumerSetting setting,
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        EzyMosquittoRpcServer server = EzyMosquittoRpcServer
            .builder()
            .mqttClient(mqttClient)
            .topic(setting.getTopic())
            .mqttCallbackProxy(mqttCallbackProxy)
            .build();
        return EzyMosquittoRpcConsumer
            .builder()
            .dataCodec(dataCodec)
            .requestInterceptors(setting.getRequestInterceptors())
            .requestHandlers(setting.getRequestHandlers())
            .threadPoolSize(setting.getThreadPoolSize())
            .server(server)
            .build();
    }

    public void close() {
        for (EzyMosquittoRpcConsumer consumer : rpcConsumers.values()) {
            processWithLogException(consumer::close);
        }
    }
}
