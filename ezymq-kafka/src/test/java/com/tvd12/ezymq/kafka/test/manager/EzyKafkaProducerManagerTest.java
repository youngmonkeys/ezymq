package com.tvd12.ezymq.kafka.test.manager;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.manager.EzyKafkaProducerManager;
import com.tvd12.ezymq.kafka.setting.EzyKafkaProducerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyKafkaProducerManagerTest extends BaseTest {

    @Test
    public void test() {
        // given
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String producerName = RandomUtil.randomShortAlphabetString();
        EzyKafkaProducerSetting producerSetting = EzyKafkaProducerSetting
            .builder()
            .topic(topic)
            .build();

        EzyKafkaProducer producerTest = mock(EzyKafkaProducer.class);

        EzyKafkaProducerManager manager = new EzyKafkaProducerManager(
            entityCodec,
            EzyMapBuilder.mapBuilder()
                .put(producerName, producerSetting)
                .toMap()
        ) {
            @Override
            protected EzyKafkaProducer createProducer(
                EzyKafkaProducerSetting setting
            ) {
                return producerTest;
            }
        };

        // given
        Asserts.assertEquals(
            manager.getProducer(producerName),
            producerTest
        );
        manager.close();
        verify(producerTest, times(1)).close();
    }

    @Test
    public void createProducerFailedTest() {
        // given
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String producerName = RandomUtil.randomShortAlphabetString();
        EzyKafkaProducerSetting producerSetting = EzyKafkaProducerSetting
            .builder()
            .topic(topic)
            .build();

        Throwable e = Asserts.assertThrows(() -> new EzyKafkaProducerManager(
            entityCodec,
            EzyMapBuilder.mapBuilder()
                .put(producerName, producerSetting)
                .toMap()
        ) {
            @Override
            protected EzyKafkaProducer createProducer(
                EzyKafkaProducerSetting setting
            ) {
                throw new RuntimeException("test");
            }
        });

        // given
        Asserts.assertEqualsType(e, IllegalStateException.class);
    }
}
