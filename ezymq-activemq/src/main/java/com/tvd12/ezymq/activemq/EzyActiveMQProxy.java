package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcConsumerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcProducerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveTopicManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;
import com.tvd12.ezymq.common.EzyMQRpcProxy;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;

import javax.jms.Connection;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyActiveMQProxy extends EzyMQRpcProxy<EzyActiveSettings> {

    protected final Connection connection;
    protected final EzyActiveTopicManager topicManager;
    protected final EzyActiveRpcProducerManager rpcProducerManager;
    protected final EzyActiveRpcConsumerManager rpcConsumerManager;

    public EzyActiveMQProxy(
        Connection connection,
        EzyActiveSettings settings,
        EzyMQDataCodec dataCodec,
        EzyEntityCodec entityCodec
    ) {
        super(settings, dataCodec, entityCodec);
        this.connection = connection;
        this.topicManager = newTopicManager();
        this.rpcProducerManager = newRpcProducerManager();
        this.rpcConsumerManager = newActiveRpcConsumerManager();
    }

    public static EzyActiveMQProxyBuilder builder() {
        return new EzyActiveMQProxyBuilder();
    }

    public <T> EzyActiveTopic<T> getTopic(String name) {
        return topicManager.getTopic(name);
    }

    public EzyActiveRpcProducer getRpcProducer(String name) {
        return rpcProducerManager.getRpcProducer(name);
    }

    public EzyActiveRpcConsumer getRpcConsumer(String name) {
        return rpcConsumerManager.getRpcConsumer(name);
    }

    @Override
    public void close() {
        topicManager.close();
        rpcConsumerManager.close();
        rpcProducerManager.close();
        processWithLogException(connection::close);
    }

    protected EzyActiveTopicManager newTopicManager() {
        return new EzyActiveTopicManager(
            connection,
            dataCodec,
            settings.getTopicSettings()
        );
    }

    protected EzyActiveRpcProducerManager newRpcProducerManager() {
        return new EzyActiveRpcProducerManager(
            connection,
            entityCodec,
            settings.getRpcProducerSettings()
        );
    }

    protected EzyActiveRpcConsumerManager newActiveRpcConsumerManager() {
        return new EzyActiveRpcConsumerManager(
            connection,
            dataCodec,
            settings.getRpcConsumerSettings()
        );
    }
}
