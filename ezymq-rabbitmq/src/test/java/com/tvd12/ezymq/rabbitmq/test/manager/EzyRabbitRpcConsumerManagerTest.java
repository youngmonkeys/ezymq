package com.tvd12.ezymq.rabbitmq.test.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcConsumerManager;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcConsumerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.*;

public class EzyRabbitRpcConsumerManagerTest extends BaseTest {

    @Test
    public void test() throws IOException {
        // given
        Connection connection = mock(Connection.class);
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);

        String exchange = RandomUtil.randomShortAlphabetString();
        String requestQueueName = RandomUtil.randomShortAlphabetString();
        String replyRoutingKey = RandomUtil.randomShortAlphabetString();
        Channel channel = mock(Channel.class);
        EzyRabbitRpcConsumerSetting consumerSetting = EzyRabbitRpcConsumerSetting
            .builder()
            .channel(channel)
            .exchange(exchange)
            .requestQueueName(requestQueueName)
            .replyRoutingKey(replyRoutingKey)
            .build();
        String consumerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings =
            EzyMapBuilder.mapBuilder()
                .put(consumerName, consumerSetting)
                .toMap();

        // when
        EzyRabbitRpcConsumerManager sut = new EzyRabbitRpcConsumerManager(
            connection,
            dataCodec,
            rpcConsumerSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getRpcConsumer(consumerName)
        );
        Asserts.assertThatThrows(() -> sut.getRpcConsumer("not found"))
                .isEqualsType(IllegalArgumentException.class);
        verify(channel, times(1)).basicQos(
            consumerSetting.getPrefetchCount()
        );
    }

    @Test
    public void createRpcConsumerFailed() {
        // given
        Connection connection = mock(Connection.class);
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);

        String requestQueueName = RandomUtil.randomShortAlphabetString();
        String replyRoutingKey = RandomUtil.randomShortAlphabetString();
        EzyRabbitRpcConsumerSetting consumerSetting = EzyRabbitRpcConsumerSetting
            .builder()
            .requestQueueName(requestQueueName)
            .replyRoutingKey(replyRoutingKey)
            .build();
        String consumerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyRabbitRpcConsumerSetting> rpcConsumerSettings =
            EzyMapBuilder.mapBuilder()
                .put(consumerName, consumerSetting)
                .toMap();

        // when
        Throwable e = Asserts.assertThrows(() ->
            new EzyRabbitRpcConsumerManager(
                connection,
                dataCodec,
                rpcConsumerSettings
            )
        );

        // then
        Asserts.assertEqualsType(e, IllegalStateException.class);
    }
}
