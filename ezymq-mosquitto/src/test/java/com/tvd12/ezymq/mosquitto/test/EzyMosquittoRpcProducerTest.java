package com.tvd12.ezymq.mosquitto.test;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.EzyTimeoutException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcProducer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoMessage;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcClient;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

public class EzyMosquittoRpcProducerTest extends BaseTest {

    @Test
    public void fireTest() throws Exception {
        // given
        EzyMosquittoRpcClient client = mock(EzyMosquittoRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyMosquittoRpcProducer sut = EzyMosquittoRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        // when
        sut.fire(data);

        // then
        Asserts.assertThatThrows(() ->
            sut.fire("data")
        ).isEqualsType(IllegalArgumentException.class);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doFire(
            any(EzyMosquittoProperties.class),
            any(byte[].class)
        );
        sut.close();
        verify(client, times(1)).close();
    }

    @Test
    public void fireFailedTest() throws Exception {
        // given
        EzyMosquittoRpcClient client = mock(EzyMosquittoRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyMosquittoRpcProducer sut = EzyMosquittoRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(client).doFire(
            any(EzyMosquittoProperties.class),
            any(byte[].class)
        );

        // when
        Throwable e = Asserts.assertThrows(() -> sut.fire(data));

        // then
        Asserts.assertEqualsType(e, InternalServerErrorException.class);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doFire(
            any(EzyMosquittoProperties.class),
            any(byte[].class)
        );
        sut.close();
        verify(client, times(1)).close();
    }

    @Test
    public void callTest() throws Exception {
        // given
        EzyMosquittoRpcClient client = mock(EzyMosquittoRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyMosquittoRpcProducer sut = EzyMosquittoRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        EzyMosquittoProperties responseProperties = EzyMosquittoProperties.builder()
            .build();
        String responseString = RandomUtil.randomShortAlphabetString();
        byte[] responseBytes = responseString.getBytes();
        EzyMosquittoMessage responseMessage = new EzyMosquittoMessage(
            responseProperties,
            responseBytes
        );
        when(
            client.doCall(any(EzyMosquittoProperties.class), any(byte[].class))
        ).thenReturn(responseMessage);

        when(
            entityCodec.deserialize(responseBytes, String.class)
        ).thenReturn(responseString);

        // when
        String result = sut.call(data, String.class);

        // then
        Asserts.assertEquals(result, new String(responseBytes));
        Asserts.assertThatThrows(() ->
            sut.call("data", String.class)
        ).isEqualsType(IllegalArgumentException.class);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doCall(
            any(EzyMosquittoProperties.class),
            any(byte[].class)
        );
        verify(entityCodec, times(1)).deserialize(
            responseBytes,
            String.class
        );
        sut.close();
        verify(client, times(1)).close();
    }

    @Test
    public void callFailedDueToTimeoutOutTest() throws Exception {
        // given
        EzyMosquittoRpcClient client = mock(EzyMosquittoRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyMosquittoRpcProducer sut = EzyMosquittoRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        TimeoutException exception = new TimeoutException("test");
        when(
            client.doCall(any(EzyMosquittoProperties.class), any(byte[].class))
        ).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() -> sut.call(data, String.class));

        // then
        Asserts.assertEqualsType(e, EzyTimeoutException.class);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doCall(
            any(EzyMosquittoProperties.class),
            any(byte[].class)
        );
        sut.close();
        verify(client, times(1)).close();
    }

    @Test
    public void callFailedDueToInternalServerErrorTest() throws Exception {
        // given
        EzyMosquittoRpcClient client = mock(EzyMosquittoRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyMosquittoRpcProducer sut = EzyMosquittoRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        RuntimeException exception = new RuntimeException("test");
        when(
            client.doCall(any(EzyMosquittoProperties.class), any(byte[].class))
        ).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() -> sut.call(data, String.class));

        // then
        Asserts.assertEqualsType(e, InternalServerErrorException.class);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doCall(
            any(EzyMosquittoProperties.class),
            any(byte[].class)
        );
        sut.close();
        verify(client, times(1)).close();
    }

    protected static class InternalData implements EzyMessageTypeFetcher {
        @Override
        public String getMessageType() {
            return "test";
        }
    }
}
