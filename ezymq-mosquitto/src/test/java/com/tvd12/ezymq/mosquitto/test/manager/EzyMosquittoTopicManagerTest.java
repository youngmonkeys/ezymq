package com.tvd12.ezymq.mosquitto.test.manager;

import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.manager.EzyMosquittoTopicManager;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoTopicSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Mockito.*;

public class EzyMosquittoTopicManagerTest extends BaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        String topicName = RandomUtil.randomShortAlphabetString();
        EzyMosquittoTopicSetting topicSetting = EzyMosquittoTopicSetting.builder()
            .topic(topicName)
            .producerEnable(true)
            .consumerEnable(true)
            .build();
        Map<String, EzyMosquittoTopicSetting> topicSettings = EzyMapBuilder
            .mapBuilder()
            .put(topicName, topicSetting)
            .build();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        // when
        EzyMosquittoTopicManager sut = new EzyMosquittoTopicManager(
            mqttClient,
            dataCodec,
            topicSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getTopic(topicName)
        );
        Asserts.assertThatThrows(() -> sut.getTopic("not found"))
            .isEqualsType(IllegalArgumentException.class);

        verify(mqttClient, times(1)).subscribe(topicName);
        verifyNoMoreInteractions(mqttClient);
        verifyNoMoreInteractions(dataCodec);
        sut.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void createTopicFailed() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        String topicName = RandomUtil.randomShortAlphabetString();
        EzyMosquittoTopicSetting topicSetting = EzyMosquittoTopicSetting.builder()
            .topic(topicName)
            .producerEnable(true)
            .consumerEnable(true)
            .build();
        Map<String, EzyMosquittoTopicSetting> topicSettings = EzyMapBuilder
            .mapBuilder()
            .put(topicName, topicSetting)
            .build();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(mqttClient).subscribe(topicName);

        // when
        Throwable e = Asserts.assertThrows(() -> {
            EzyMosquittoTopicManager sut = new EzyMosquittoTopicManager(
                mqttClient,
                dataCodec,
                topicSettings
            );
            sut.close();
        });

        // then
        Asserts.assertEqualsType(e, IllegalStateException.class);

        verify(mqttClient, times(1)).subscribe(topicName);
        verifyNoMoreInteractions(mqttClient);
        verifyNoMoreInteractions(dataCodec);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void buildWithDisableProducerAndConsumer() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        String topicName = RandomUtil.randomShortAlphabetString();
        EzyMosquittoTopicSetting topicSetting = EzyMosquittoTopicSetting.builder()
            .topic(topicName)
            .build();
        Map<String, EzyMosquittoTopicSetting> topicSettings = EzyMapBuilder
            .mapBuilder()
            .put(topicName, topicSetting)
            .build();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        // when
        EzyMosquittoTopicManager sut = new EzyMosquittoTopicManager(
            mqttClient,
            dataCodec,
            topicSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getTopic(topicName)
        );
        Asserts.assertThatThrows(() -> sut.getTopic("not found"))
            .isEqualsType(IllegalArgumentException.class);

        verify(mqttClient, times(1)).subscribe(topicName);
        verifyNoMoreInteractions(mqttClient);
        verifyNoMoreInteractions(dataCodec);
        sut.close();
    }
}
