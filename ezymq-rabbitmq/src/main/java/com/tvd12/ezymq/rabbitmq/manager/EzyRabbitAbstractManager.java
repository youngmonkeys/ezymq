package com.tvd12.ezymq.rabbitmq.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitEndpointSetting;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EzyRabbitAbstractManager extends EzyLoggable {

    protected final Connection connection;

    public Channel getChannel(EzyRabbitEndpointSetting setting) throws Exception {
        Channel channel = setting.getChannel();
        if (channel == null) {
            channel = connection.createChannel();
        }
        return channel;
    }
}
