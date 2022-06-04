package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezymq.kafka.EzyKafkaProducer;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaClient;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyKafkaProducerTest extends BaseTest {

    @Test
    public void sendTest() {
        // given
        EzyKafkaClient client = mock(EzyKafkaClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        InternalData data = new InternalData();
        byte[] bytes = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(bytes);

        EzyKafkaProducer sut = EzyKafkaProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        // when
        sut.send(data);

        // then
        verify(client, times(1)).send("test", bytes);

        sut.close();
        verify(client, times(1)).close();
    }

    @Test
    public void sendFailedDueToInternalServerErrorException() {
        // given
        EzyKafkaClient client = mock(EzyKafkaClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        String data = RandomUtil.randomShortAlphabetString();
        byte[] bytes = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(bytes);

        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(client).send("", bytes);

        EzyKafkaProducer sut = EzyKafkaProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.send(data)
        );

        // then
        Asserts.assertEqualsType(e, InternalServerErrorException.class);
        verify(client, times(1)).send("", bytes);
    }

    private static class InternalData implements EzyMessageTypeFetcher {
        @Override
        public String getMessageType() {
            return "test";
        }
    }
}
