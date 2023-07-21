package com.tvd12.ezymq.mosquitto.test.manager;

import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallback;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoRpcConsumerManager;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoRpcConsumerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Mockito.*;

public class EzyMosquittoRpcConsumerManagerTest extends BaseTest {

    @Test
    public void test() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();
        EzyMosquittoRpcConsumerSetting consumerSetting = EzyMosquittoRpcConsumerSetting
            .builder()
            .topic(topic)
            .replyTopic(replyTopic)
            .build();
        String consumerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings =
            EzyMapBuilder.mapBuilder()
                .put(consumerName, consumerSetting)
                .toMap();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        // when
        EzyMosquittoRpcConsumerManager sut = new EzyMosquittoRpcConsumerManager(
            mqttClient,
            dataCodec,
            rpcConsumerSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getRpcConsumer(consumerName)
        );
        Asserts.assertThatThrows(() -> sut.getRpcConsumer("not found"))
                .isEqualsType(IllegalArgumentException.class);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).subscribe(topic);
        verifyNoMoreInteractions(mqttClient);

        verifyNoMoreInteractions(dataCodec);
        sut.close();
    }

    @Test
    public void createRpcConsumerFailed() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();
        EzyMosquittoRpcConsumerSetting consumerSetting = EzyMosquittoRpcConsumerSetting
            .builder()
            .topic(topic)
            .replyTopic(replyTopic)
            .build();
        String consumerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyMosquittoRpcConsumerSetting> rpcConsumerSettings =
            EzyMapBuilder.mapBuilder()
                .put(consumerName, consumerSetting)
                .toMap();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(mqttClient).subscribe(topic);

        // when
        Throwable e = Asserts.assertThrows(() ->
            new EzyMosquittoRpcConsumerManager(
                mqttClient,
                dataCodec,
                rpcConsumerSettings
            )
        );

        // then
        Asserts.assertEqualsType(e, IllegalStateException.class);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).subscribe(topic);
        verifyNoMoreInteractions(mqttClient);

        verifyNoMoreInteractions(dataCodec);
    }
}
