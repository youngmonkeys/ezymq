package com.tvd12.ezymq.rabbitmq.testing;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.tvd12.ezyfox.io.EzyMaps;
import com.tvd12.ezymq.rabbitmq.factory.EzyRabbitSimpleCorrelationIdFactory;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitResponseConsumer;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcProducerSetting;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcConsumerSetting;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitTopicSetting;
import com.tvd12.ezymq.rabbitmq.testing.mockup.ChannelMockup;
import org.testng.annotations.Test;

public class EzyRabbitSettingsTest {

    @Test
    public void test() {
        Channel channel = new ChannelMockup();
        EzyRabbitSettings settings = EzyRabbitSettings.builder()
            .queueArgument("a", "hello", "world")
            .queueArguments("a", EzyMaps.newHashMap("foo", "bar"))
            .addTopicSetting("topic", EzyRabbitTopicSetting.builder()
                .channel(channel)
                .build())
            .addRpcProducerSetting("rpcconsumer", EzyRabbitRpcProducerSetting.builder()
                .channel(channel)
                .capacity(100)
                .correlationIdFactory(new EzyRabbitSimpleCorrelationIdFactory())
                .unconsumedResponseConsumer(new EzyRabbitResponseConsumer() {

                    @Override
                    public void consume(BasicProperties properties, byte[] responseBody) {
                    }
                })
                .build())
            .addRpcConsumerSetting("rpchandler", EzyRabbitRpcConsumerSetting.builder()
                .channel(channel)
                .threadPoolSize(8)
                .prefetchCount(100)
                .requestHandlers(new EzyRabbitRequestHandlers())
                .addRequestHandler(EzyMaps.newHashMap("a", v -> 1))
                .build())
            .build();
        assert settings.getQueueArguments().get("a").size() == 2;
        assert settings.getTopicSettings().get("topic") != null;
        assert settings.getRpcProducerSettings().get("rpcconsumer") != null;
        assert settings.getRpcConsumerSettings().get("rpchandler") != null;
    }

}
