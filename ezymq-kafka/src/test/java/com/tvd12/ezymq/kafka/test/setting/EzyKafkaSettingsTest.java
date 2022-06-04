package com.tvd12.ezymq.kafka.test.setting;

import com.tvd12.ezyfox.collect.Sets;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.setting.EzyKafkaConsumerSetting;
import com.tvd12.ezymq.kafka.setting.EzyKafkaProducerSetting;
import com.tvd12.ezymq.kafka.setting.EzyKafkaSettings;
import com.tvd12.ezymq.kafka.test.handler.MultiplyRequestHandler;
import com.tvd12.ezymq.kafka.test.handler.SumRequestHandler;
import com.tvd12.ezymq.kafka.test.request.MultiplyRequest;
import com.tvd12.ezymq.kafka.test.request.SumRequest;
import com.tvd12.properties.file.reader.BaseFileReader;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
public class EzyKafkaSettingsTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyKafkaMessageInterceptor interceptor1 = mock(EzyKafkaMessageInterceptor.class);
        EzyKafkaMessageInterceptor interceptor2 = mock(EzyKafkaMessageInterceptor.class);
        EzyKafkaMessageInterceptor interceptor3 = mock(EzyKafkaMessageInterceptor.class);

        SumRequestHandler sumRequestHandler = new SumRequestHandler();
        MultiplyRequestHandler multiplyRequestHandler = new MultiplyRequestHandler();

        // when
        EzyKafkaSettings sut = EzyKafkaSettings.builder()
            .mapMessageType("topic1", String.class)
            .mapMessageTypes(
                "topic1",
                EzyMapBuilder.mapBuilder()
                    .put("boolean", Boolean.class)
                    .put("byte", Byte.class)
                    .toMap()
            )
            .mapMessageTypes(
                EzyMapBuilder.mapBuilder()
                    .put(
                        "topic1",
                        Collections.singletonMap("char", Character.class)
                    )
                    .put(
                        "topic2",
                        EzyMapBuilder.mapBuilder()
                            .put("double", Double.class)
                            .put("float", Float.class)
                            .build()
                    )
                    .build()
            )
            .addConsumerInterceptor(interceptor1)
            .addConsumerInterceptors(Arrays.asList(interceptor2, interceptor3))
            .addConsumerMessageHandlers(
                Arrays.asList(sumRequestHandler, multiplyRequestHandler)
            )
            .producerSettingBuilder("producer1")
            .parent()
            .consumerSettingBuilder("consumer1")
            .parent()
            .addProducerSetting(
                "producer2",
                EzyKafkaProducerSetting.builder()
                    .topic("test")
                    .build()
            )
            .addConsumerSetting(
                "consumer2",
                EzyKafkaConsumerSetting.builder()
                    .topic("test")
                    .build()
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
            sut.getMessageTypesByTopic(),
            EzyMapBuilder.mapBuilder()
                .put(
                    "topic1",
                    EzyMapBuilder.mapBuilder()
                        .put("", String.class)
                        .put("boolean", Boolean.class)
                        .put("byte", Byte.class)
                        .put("char", Character.class)
                        .build()
                )
                .put(
                    "topic2",
                    EzyMapBuilder.mapBuilder()
                        .put("double", Double.class)
                        .put("float", Float.class)
                        .build()
                )
                .put(
                    "test",
                    EzyMapBuilder.mapBuilder()
                        .put("sum", SumRequest.class)
                        .put("multi", MultiplyRequest.class)
                        .toMap()
                )
                .build(),
            false
        );

        Map<String, EzyKafkaProducerSetting> producerSettings = sut
            .getProducerSettings();
        Asserts.assertEquals(producerSettings.size(), 2);

        Map<String, EzyKafkaConsumerSetting> consumerSettings = sut
            .getConsumerSettings();
        Asserts.assertEquals(consumerSettings.size(), 2);
    }

    @Test
    public void buildFromProperties() {
        // given
        Properties properties = new BaseFileReader()
            .read("application-for-test-settings.yaml");

        // when
        EzyKafkaSettings sut = EzyKafkaSettings.builder()
            .properties(properties)
            .build();

        // then
        Asserts.assertEquals(
            sut.getMessageTypes(),
            Sets.newHashSet(
                Boolean.class,
                Byte.class,
                String.class
            )
        );
        Asserts.assertEquals(
            sut.getMessageTypesByTopic(),
            EzyMapBuilder.mapBuilder()
                .put(
                    "test",
                    EzyMapBuilder.mapBuilder()
                        .put("", String.class)
                        .put("command1", Boolean.class)
                        .put("command2", Byte.class)
                        .build()
                )
                .build(),
            false
        );

        Map<String, EzyKafkaProducerSetting> producerSettings = sut
            .getProducerSettings();
        Asserts.assertEquals(producerSettings.size(), 1);

        EzyKafkaProducerSetting producerSetting = producerSettings.get(
            "producer1"
        );
        Asserts.assertEquals(producerSetting.getTopic(), "test");
        Asserts.assertEquals(
            producerSetting.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
            "127.0.0.1:9091"
        );
        Asserts.assertEquals(
            producerSetting.getProperty(ProducerConfig.CLIENT_ID_CONFIG),
            "KafkaProducerExample"
        );

        Map<String, EzyKafkaConsumerSetting> consumerSettings = sut
            .getConsumerSettings();
        Asserts.assertEquals(consumerSettings.size(), 1);

        EzyKafkaConsumerSetting consumerSetting = consumerSettings.get(
            "consumer1"
        );
        Asserts.assertEquals(consumerSetting.getTopic(), "test");
        Asserts.assertEquals(
            consumerSetting.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
            "127.0.0.1:9091"
        );
        Asserts.assertEquals(
            consumerSetting.getProperty(ConsumerConfig.GROUP_ID_CONFIG),
            "KafkaConsumerExample"
        );
    }

    @Test
    public void buildParentTest() {
        // given
        EzyKafkaSettings.Builder builder = EzyKafkaSettings.builder();

        // when
        // then
        Asserts.assertNull(builder.parent());
    }
}
