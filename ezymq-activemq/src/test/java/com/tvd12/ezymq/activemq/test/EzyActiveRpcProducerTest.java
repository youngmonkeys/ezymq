package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.exception.EzyTimeoutException;
import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveMessage;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

public class EzyActiveRpcProducerTest extends BaseTest {

    @Test
    public void fireTest() throws Exception {
        // given
        EzyActiveRpcClient client = mock(EzyActiveRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyActiveRpcProducer sut = EzyActiveRpcProducer.builder()
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
            any(EzyActiveProperties.class),
            any(byte[].class)
        );
        sut.close();
        verify(client, times(1)).close();
    }

    @Test
    public void fireFailedTest() throws Exception {
        // given
        EzyActiveRpcClient client = mock(EzyActiveRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyActiveRpcProducer sut = EzyActiveRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(client).doFire(
            any(EzyActiveProperties.class),
            any(byte[].class)
        );

        // when
        Throwable e = Asserts.assertThrows(() -> sut.fire(data));

        // then
        Asserts.assertEqualsType(e, InternalServerErrorException.class);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doFire(
            any(EzyActiveProperties.class),
            any(byte[].class)
        );
        sut.close();
        verify(client, times(1)).close();
    }

    @Test
    public void callTest() throws Exception {
        // given
        EzyActiveRpcClient client = mock(EzyActiveRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyActiveRpcProducer sut = EzyActiveRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        EzyActiveProperties responseProperties = EzyActiveProperties.builder()
            .build();
        String responseString = RandomUtil.randomShortAlphabetString();
        byte[] responseBytes = responseString.getBytes();
        EzyActiveMessage responseMessage = new EzyActiveMessage(
            responseProperties,
            responseBytes
        );
        when(
            client.doCall(any(EzyActiveProperties.class), any(byte[].class))
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
            any(EzyActiveProperties.class),
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
        EzyActiveRpcClient client = mock(EzyActiveRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyActiveRpcProducer sut = EzyActiveRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        TimeoutException exception = new TimeoutException("test");
        when(
            client.doCall(any(EzyActiveProperties.class), any(byte[].class))
        ).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() -> sut.call(data, String.class));

        // then
        Asserts.assertEqualsType(e, EzyTimeoutException.class);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doCall(
            any(EzyActiveProperties.class),
            any(byte[].class)
        );
        sut.close();
        verify(client, times(1)).close();
    }

    @Test
    public void callFailedDueToInternalServerErrorTest() throws Exception {
        // given
        EzyActiveRpcClient client = mock(EzyActiveRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyActiveRpcProducer sut = EzyActiveRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        RuntimeException exception = new RuntimeException("test");
        when(
            client.doCall(any(EzyActiveProperties.class), any(byte[].class))
        ).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() -> sut.call(data, String.class));

        // then
        Asserts.assertEqualsType(e, InternalServerErrorException.class);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doCall(
            any(EzyActiveProperties.class),
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
