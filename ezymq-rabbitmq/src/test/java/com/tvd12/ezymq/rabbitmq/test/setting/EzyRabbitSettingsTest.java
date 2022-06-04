package com.tvd12.ezymq.rabbitmq.test.setting;

import com.tvd12.ezyfox.collect.Sets;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptor;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcConsumerSetting;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcProducerSetting;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitTopicSetting;
import com.tvd12.ezymq.rabbitmq.test.handler.MultiplyRequestHandler;
import com.tvd12.ezymq.rabbitmq.test.handler.SumRequestHandler;
import com.tvd12.ezymq.rabbitmq.test.handler.SumRequestMessageHandler;
import com.tvd12.ezymq.rabbitmq.test.request.MultiplyRequest;
import com.tvd12.ezymq.rabbitmq.test.request.SumRequest;
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
public class EzyRabbitSettingsTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyRabbitRequestInterceptor interceptor1 = mock(EzyRabbitRequestInterceptor.class);
        EzyRabbitRequestInterceptor interceptor2 = mock(EzyRabbitRequestInterceptor.class);
        EzyRabbitRequestInterceptor interceptor3 = mock(EzyRabbitRequestInterceptor.class);

        SumRequestHandler sumRequestHandler = new SumRequestHandler();
        MultiplyRequestHandler multiplyRequestHandler = new MultiplyRequestHandler();

        // when
        EzyRabbitSettings sut = EzyRabbitSettings.builder()
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
                EzyRabbitRpcProducerSetting.builder()
                    .build()
            )
            .addRpcConsumerSetting(
                "consumer2",
                EzyRabbitRpcConsumerSetting.builder()
                    .build()
            )
            .topicSettingBuilder("topic1")
            .parent()
            .addTopicSetting(
                "topic2",
                EzyRabbitTopicSetting.builder().build()
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

        Map<String, EzyRabbitRpcProducerSetting> producerSettings = sut
            .getRpcProducerSettings();
        Asserts.assertEquals(producerSettings.size(), 2);

        Map<String, EzyRabbitRpcConsumerSetting> consumerSettings = sut
            .getRpcConsumerSettings();
        Asserts.assertEquals(consumerSettings.size(), 2);

        Map<String, EzyRabbitTopicSetting> topicSettings = sut.getTopicSettings();
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
        EzyRabbitSettings sut = EzyRabbitSettings.builder()
            .properties(properties)
            .queueArguments(
                "queue1",
                EzyMapBuilder.mapBuilder()
                    .put("hello", "world")
                    .put("foo", "bar")
                    .build()
            )
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

        Map<String, EzyRabbitRpcProducerSetting> producerSettings = sut
            .getRpcProducerSettings();
        Asserts.assertEquals(producerSettings.size(), 1);

        Map<String, EzyRabbitRpcConsumerSetting> consumerSettings = sut
            .getRpcConsumerSettings();
        Asserts.assertEquals(consumerSettings.size(), 1);

        Map<String, EzyRabbitTopicSetting> topicSettings = sut.getTopicSettings();
        Asserts.assertEquals(topicSettings.size(), 1);

        Asserts.assertEquals(
            sut.getQueueArguments(),
            EzyMapBuilder.mapBuilder()
                .put(
                    "queue1",
                    EzyMapBuilder.mapBuilder()
                        .put("hello", "world")
                        .put("foo", "bar")
                        .build()
                )
                .build(),
            false
        );
    }

    @Test
    public void buildParentTest() {
        // given
        EzyRabbitSettings.Builder builder = EzyRabbitSettings.builder();

        // when
        // then
        Asserts.assertNull(builder.parent());
    }
}
