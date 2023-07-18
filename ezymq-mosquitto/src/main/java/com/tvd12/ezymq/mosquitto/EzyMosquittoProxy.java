package com.tvd12.ezymq.mosquitto;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.common.EzyMQRpcProxy;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoRpcConsumerManager;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoRpcProducerManager;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoTopicManager;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyMosquittoProxy extends EzyMQRpcProxy<EzyMosquittoSettings> {

    protected final EzyMqttClientProxy mqttClient;
    protected final EzyMosquittoTopicManager topicManager;
    protected final EzyMosquittoRpcProducerManager rpcProducerManager;
    protected final EzyMosquittoRpcConsumerManager rpcConsumerManager;

    public EzyMosquittoProxy(
        EzyMqttClientProxy mqttClient,
        EzyMosquittoSettings settings,
        EzyMQDataCodec dataCodec,
        EzyEntityCodec entityCodec
    ) {
        super(settings, dataCodec, entityCodec);
        this.mqttClient = mqttClient;
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
            dataCodec,
            settings.getTopicSettings()
        );
    }

    protected EzyMosquittoRpcProducerManager newRpcProducerManager() {
        return new EzyMosquittoRpcProducerManager(
            mqttClient,
            entityCodec,
            settings.getRpcProducerSettings()
        );
    }

    protected EzyMosquittoRpcConsumerManager newRabbitRpcConsumerManager() {
        return new EzyMosquittoRpcConsumerManager(
            mqttClient,
            dataCodec,
            settings.getRpcConsumerSettings()
        );
    }
}
