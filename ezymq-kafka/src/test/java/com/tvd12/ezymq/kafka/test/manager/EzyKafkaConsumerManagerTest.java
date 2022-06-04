package com.tvd12.ezymq.kafka.test.manager;

import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.kafka.EzyKafkaConsumer;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.manager.EzyKafkaConsumerManager;
import com.tvd12.ezymq.kafka.setting.EzyKafkaConsumerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyKafkaConsumerManagerTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyKafkaDataCodec dataCodec = mock(EzyKafkaDataCodec.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String producerName = RandomUtil.randomShortAlphabetString();
        EzyKafkaConsumerSetting producerSetting = EzyKafkaConsumerSetting
            .builder()
            .topic(topic)
            .build();

        EzyKafkaConsumer producerTest = mock(EzyKafkaConsumer.class);

        EzyKafkaConsumerManager manager = new EzyKafkaConsumerManager(
            dataCodec,
            EzyMapBuilder.mapBuilder()
                .put(producerName, producerSetting)
                .toMap()
        ) {
            @Override
            protected EzyKafkaConsumer createConsumer(
                EzyKafkaConsumerSetting setting
            ) {
                return producerTest;
            }
        };

        // given
        Asserts.assertEquals(
            manager.getConsumer(producerName),
            producerTest
        );
        manager.close();
        verify(producerTest, times(1)).close();
    }

    @Test
    public void createConsumerFailedTest() {
        // given
        EzyKafkaDataCodec dataCodec = mock(EzyKafkaDataCodec.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String producerName = RandomUtil.randomShortAlphabetString();
        EzyKafkaConsumerSetting producerSetting = EzyKafkaConsumerSetting
            .builder()
            .topic(topic)
            .build();

        Throwable e = Asserts.assertThrows(() -> new EzyKafkaConsumerManager(
            dataCodec,
            EzyMapBuilder.mapBuilder()
                .put(producerName, producerSetting)
                .toMap()
        ) {
            @Override
            protected EzyKafkaConsumer createConsumer(
                EzyKafkaConsumerSetting setting
            ) {
                throw new RuntimeException("test");
            }
        });

        // given
        Asserts.assertEqualsType(e, IllegalStateException.class);
    }
}
