package com.tvd12.ezymq.mosquitto;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.common.EzyMQRpcProxy;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallbackProxy;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoRpcConsumerManager;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoRpcProducerManager;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoTopicManager;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;

public class EzyMosquittoProxy extends EzyMQRpcProxy<EzyMosquittoSettings> {

    protected final MqttClient mqttClient;
    protected final EzyMqttCallbackProxy mqttCallbackProxy;
    protected final EzyMosquittoTopicManager topicManager;
    protected final EzyMosquittoRpcProducerManager rpcProducerManager;
    protected final EzyMosquittoRpcConsumerManager rpcConsumerManager;

    public EzyMosquittoProxy(
        MqttClient mqttClient,
        EzyMqttCallbackProxy mqttCallbackProxy,
        EzyMosquittoSettings settings,
        EzyMQDataCodec dataCodec,
        EzyEntityCodec entityCodec
    ) {
        super(settings, dataCodec, entityCodec);
        this.mqttClient = mqttClient;
        this.mqttCallbackProxy = mqttCallbackProxy;
        this.topicManager = newTopicManager();
        this.rpcProducerManager = newRpcProducerManager();
        this.rpcConsumerManager = newRabbitRpcConsumerManager();
    }

    public static EzyMosquittoProxyBuilder builder() {
        return new EzyMosquittoProxyBuilder();
    }

    public <T> EzyMosquittoTopic<T> getTopic(String name) {
        return topicManager.getTopic(name);
    }

    public EzyMosquittoRpcProducer getRpcProducer(String name) {
        return rpcProducerManager.getRpcProducer(name);
    }

    public EzyMosquittoRpcConsumer getRpcConsumer(String name) {
        return rpcConsumerManager.getRpcConsumer(name);
    }

    @Override
    public void close() {
        topicManager.close();
        rpcConsumerManager.close();
        rpcProducerManager.close();
        processWithLogException(mqttClient::close);
    }

    protected EzyMosquittoTopicManager newTopicManager() {
        return new EzyMosquittoTopicManager(
            mqttClient,
            mqttCallbackProxy,
            dataCodec,
            settings.getTopicSettings()
        );
    }

    protected EzyMosquittoRpcProducerManager newRpcProducerManager() {
        return new EzyMosquittoRpcProducerManager(
            mqttClient,
            mqttCallbackProxy,
            entityCodec,
            settings.getRpcProducerSettings()
        );
    }

    protected EzyMosquittoRpcConsumerManager newRabbitRpcConsumerManager() {
        return new EzyMosquittoRpcConsumerManager(
            mqttClient,
            mqttCallbackProxy,
            dataCodec,
            settings.getRpcConsumerSettings()
        );
    }
}
