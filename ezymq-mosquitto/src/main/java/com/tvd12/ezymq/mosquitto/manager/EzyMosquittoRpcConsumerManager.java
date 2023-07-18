package com.tvd12.ezymq.mosquitto.manager;

import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcConsumer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcServer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoRpcConsumerSetting;

import java.util.HashMap;
import java.util.Map;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyMosquittoRpcConsumerManager extends EzyMosquittoAbstractManager {

    protected final EzyMQDataCodec dataCodec;
    protected final Map<String, EzyMosquittoRpcConsumer> rpcConsumers;
    protected final Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings;

    public EzyMosquittoRpcConsumerManager(
        EzyMqttClientProxy mqttClient,
        EzyMQDataCodec dataCodec,
        Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings
    ) {
        super(mqttClient);
        this.dataCodec = dataCodec;
        this.rpcConsumerSettings = rpcConsumerSettings;
        this.rpcConsumers = createRpcConsumers();
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

    protected Map<String, EzyMosquittoRpcConsumer> createRpcConsumers() {
        Map<String, EzyMosquittoRpcConsumer> map = new HashMap<>();
        for (String name : rpcConsumerSettings.keySet()) {
            EzyMosquittoRpcConsumerSetting setting = rpcConsumerSettings.get(name);
            map.put(name, createRpcConsumer(name, setting));
        }
        return map;
    }

    protected EzyMosquittoRpcConsumer createRpcConsumer(
        String name,
        EzyMosquittoRpcConsumerSetting setting
    ) {
        try {
            return createRpcConsumer(setting);
        } catch (Exception e) {
            throw new IllegalStateException(
                "can't create rpc consumer: " + name,
                e
            );
        }
    }

    protected EzyMosquittoRpcConsumer createRpcConsumer(
        EzyMosquittoRpcConsumerSetting setting
    ) throws Exception {
        String topic = setting.getTopic();
        EzyMosquittoRpcServer server = EzyMosquittoRpcServer
            .builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .build();
        EzyMosquittoRpcConsumer consumer = EzyMosquittoRpcConsumer
            .builder()
            .dataCodec(dataCodec)
            .requestInterceptors(setting.getRequestInterceptors())
            .requestHandlers(setting.getRequestHandlers())
            .threadPoolSize(setting.getThreadPoolSize())
            .server(server)
            .build();
        mqttClient.subscribe(topic);
        return consumer;
    }

    public void close() {
        for (EzyMosquittoRpcConsumer consumer : rpcConsumers.values()) {
            processWithLogException(consumer::close);
        }
    }
}
