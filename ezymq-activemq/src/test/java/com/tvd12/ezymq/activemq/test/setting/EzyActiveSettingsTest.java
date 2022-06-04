package com.tvd12.ezymq.activemq.test.setting;

import com.tvd12.ezyfox.collect.Sets;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptor;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcConsumerSetting;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcProducerSetting;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;
import com.tvd12.ezymq.activemq.setting.EzyActiveTopicSetting;
import com.tvd12.ezymq.activemq.test.handler.MultiplyRequestHandler;
import com.tvd12.ezymq.activemq.test.handler.SumRequestHandler;
import com.tvd12.ezymq.activemq.test.handler.SumRequestMessageHandler;
import com.tvd12.ezymq.activemq.test.request.MultiplyRequest;
import com.tvd12.ezymq.activemq.test.request.SumRequest;
import com.tvd12.properties.file.reader.BaseFileReader;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
public class EzyActiveSettingsTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyActiveRequestInterceptor interceptor1 = mock(EzyActiveRequestInterceptor.class);
        EzyActiveRequestInterceptor interceptor2 = mock(EzyActiveRequestInterceptor.class);
        EzyActiveRequestInterceptor interceptor3 = mock(EzyActiveRequestInterceptor.class);

        SumRequestHandler sumRequestHandler = new SumRequestHandler();
        MultiplyRequestHandler multiplyRequestHandler = new MultiplyRequestHandler();

        // when
        EzyActiveSettings sut = EzyActiveSettings.builder()
            .mapRequestType("command1", String.class)
            .mapRequestTypes(
                EzyMapBuilder.mapBuilder()
                    .put("boolean", Boolean.class)
                    .put("byte", Byte.class)
                    .toMap()
            )
            .mapTopicMessageType("topic1", "command1", String.class)
            .mapTopicMessageTypes(
                "topic1",
                EzyMapBuilder.mapBuilder()
                    .put("", String.class)
                    .put("boolean", Boolean.class)
                    .put("byte", Byte.class)
                    .put("char", Character.class)
                    .build()
            )
            .mapTopicMessageTypes(
                EzyMapBuilder.mapBuilder()
                    .put(
                        "topic2",
                        EzyMapBuilder.mapBuilder()
                            .put("double", Double.class)
                            .put("float", Float.class)
                            .build()
                    )
                    .build()
            )
            .addRequestInterceptor(interceptor1)
            .addRequestInterceptors(Arrays.asList(interceptor2, interceptor3))
            .addRequestHandlers(
                Arrays.asList(sumRequestHandler, multiplyRequestHandler)
            )
            .rpcProducerSettingBuilder("producer1")
            .parent()
            .rpcConsumerSettingBuilder("consumer1")
            .parent()
            .addRpcProducerSetting(
                "producer2",
                EzyActiveRpcProducerSetting.builder()
                    .build()
            )
            .addRpcConsumerSetting(
                "consumer2",
                EzyActiveRpcConsumerSetting.builder()
                    .build()
            )
            .topicSettingBuilder("topic1")
            .parent()
            .addTopicSetting(
                "topic2",
                EzyActiveTopicSetting.builder().build()
            )
            .build();

        // then
        Asserts.assertEquals(
            sut.getMessageTypes(),
            Sets.newHashSet(
                Boolean.class,
                Byte.class,
                Character.class,
                Double.class,
                Float.class,
                String.class,
                SumRequest.class,
                MultiplyRequest.class
            )
        );
        Asserts.assertEquals(
            sut.getMessageTypeMapByTopic(),
            EzyMapBuilder.mapBuilder()
                .put(
                    "topic1",
                    EzyMapBuilder.mapBuilder()
                        .put("", String.class)
                        .put("boolean", Boolean.class)
                        .put("byte", Byte.class)
                        .put("char", Character.class)
                        .put("command1", String.class)
                        .build()
                )
                .put(
                    "topic2",
                    EzyMapBuilder.mapBuilder()
                        .put("double", Double.class)
                        .put("float", Float.class)
                        .build()
                )
                .build(),
            false
        );

        Map<String, EzyActiveRpcProducerSetting> producerSettings = sut
            .getRpcProducerSettings();
        Asserts.assertEquals(producerSettings.size(), 2);

        Map<String, EzyActiveRpcConsumerSetting> consumerSettings = sut
            .getRpcConsumerSettings();
        Asserts.assertEquals(consumerSettings.size(), 2);

        Map<String, EzyActiveTopicSetting> topicSettings = sut.getTopicSettings();
        Asserts.assertEquals(topicSettings.size(), 2);
    }

    @Test
    public void buildFromProperties() {
        // given
        Properties properties = new BaseFileReader()
            .read("application-for-test-settings.yaml");
        SumRequestHandler sumRequestHandler = new SumRequestHandler();
        MultiplyRequestHandler multiplyRequestHandler = new MultiplyRequestHandler();

        // when
        EzyActiveSettings sut = EzyActiveSettings.builder()
            .properties(properties)
            .addRequestHandlers(
                Arrays.asList(sumRequestHandler, multiplyRequestHandler)
            )
            .addMessageConsumers(
                Collections.singletonList(new SumRequestMessageHandler())
            )
            .build();

        // then
        Asserts.assertEquals(
            sut.getMessageTypes(),
            Sets.newHashSet(
                SumRequest.class,
                MultiplyRequest.class
            )
        );

        Map<String, EzyActiveRpcProducerSetting> producerSettings = sut
            .getRpcProducerSettings();
        Asserts.assertEquals(producerSettings.size(), 1);

        Map<String, EzyActiveRpcConsumerSetting> consumerSettings = sut
            .getRpcConsumerSettings();
        Asserts.assertEquals(consumerSettings.size(), 1);

        Map<String, EzyActiveTopicSetting> topicSettings = sut.getTopicSettings();
        Asserts.assertEquals(topicSettings.size(), 1);
    }

    @Test
    public void buildParentTest() {
        // given
        EzyActiveSettings.Builder builder = EzyActiveSettings.builder();

        // when
        // then
        Asserts.assertNull(builder.parent());
    }
}
