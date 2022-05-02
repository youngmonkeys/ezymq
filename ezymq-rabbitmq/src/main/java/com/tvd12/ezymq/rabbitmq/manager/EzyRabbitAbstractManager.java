package com.tvd12.ezymq.rabbitmq.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitEndpointSetting;

public class EzyRabbitAbstractManager extends EzyLoggable {

    protected final ConnectionFactory connectionFactory;

    public EzyRabbitAbstractManager(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public Channel getChannel(EzyRabbitEndpointSetting setting) throws Exception {
        Channel channel = setting.getChannel();
        if (channel == null) {
            Connection connection = connectionFactory.newConnection();
            channel = connection.createChannel();
        }
        return channel;
    }
}
