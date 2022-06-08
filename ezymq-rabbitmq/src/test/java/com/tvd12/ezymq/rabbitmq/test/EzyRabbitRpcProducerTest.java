package com.tvd12.ezymq.rabbitmq.test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.RpcClient;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcProducer;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

public class EzyRabbitRpcProducerTest extends BaseTest {

    @Test
    public void callTest() throws Exception {
        // given
        EzyRabbitRpcClient client = mock(EzyRabbitRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyRabbitRpcProducer sut = EzyRabbitRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();

        AMQP.BasicProperties requestProperties = new AMQP.BasicProperties.Builder()
            .type("test")
            .build();

        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        RpcClient.Response response = mock(RpcClient.Response.class);
        byte[] responseBody = RandomUtil.randomShortByteArray();
        when(response.getBody()).thenReturn(responseBody);

        AMQP.BasicProperties responseProperties = new AMQP.BasicProperties();
        when(response.getProperties()).thenReturn(responseProperties);

        when(client.doCall(requestProperties, requestMessage)).thenReturn(response);

        String responseData = RandomUtil.randomShortAlphabetString();
        when(entityCodec.deserialize(responseBody, String.class)).thenReturn(responseData);

        // when
        String result = sut.call(data, String.class);

        // then
        Asserts.assertEquals(responseData, result);
        verify(entityCodec, times(1)).serialize(data);
        verify(entityCodec, times(1)).deserialize(
            responseBody,
            String.class
        );
        verify(client, times(1)).doCall(
            requestProperties,
            requestMessage
        );
        verify(response, times(1)).getProperties();
        verify(response, times(1)).getBody();
    }

    @Test
    public void callThrowTimeoutExceptionTest() throws Exception {
        // given
        EzyRabbitRpcClient client = mock(EzyRabbitRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyRabbitRpcProducer sut = EzyRabbitRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();

        AMQP.BasicProperties requestProperties = new AMQP.BasicProperties.Builder()
            .type("test")
            .build();

        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        TimeoutException exception = new TimeoutException("test");
        when(
            client.doCall(requestProperties, requestMessage)
        ).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.call(data, String.class)
        );

        // then
        Asserts.assertEquals(e.getCause(), exception);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doCall(
            requestProperties,
            requestMessage
        );
    }

    @Test
    public void callThrowExceptionTest() throws Exception {
        // given
        EzyRabbitRpcClient client = mock(EzyRabbitRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyRabbitRpcProducer sut = EzyRabbitRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();

        AMQP.BasicProperties requestProperties = new AMQP.BasicProperties.Builder()
            .type("test")
            .build();

        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        RuntimeException exception = new RuntimeException("test");
        when(
            client.doCall(requestProperties, requestMessage)
        ).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.call(data, String.class)
        );

        // then
        Asserts.assertEquals(e.getCause(), exception);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doCall(
            requestProperties,
            requestMessage
        );
    }

    @Test
    public void rawFireFailedTest() throws Exception {
        // given
        EzyRabbitRpcClient client = mock(EzyRabbitRpcClient.class);
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);

        EzyRabbitRpcProducer sut = EzyRabbitRpcProducer.builder()
            .client(client)
            .entityCodec(entityCodec)
            .build();

        Object data = new InternalData();

        AMQP.BasicProperties requestProperties = new AMQP.BasicProperties.Builder()
            .type("test")
            .build();

        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(entityCodec.serialize(data)).thenReturn(requestMessage);

        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(client).doFire(requestProperties, requestMessage);

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.fire("test", data)
        );

        // then
        Asserts.assertEquals(e.getCause(), exception);
        verify(entityCodec, times(1)).serialize(data);
        verify(client, times(1)).doFire(
            requestProperties,
            requestMessage
        );
    }

    private static class InternalData implements EzyMessageTypeFetcher {
        @Override
        public String getMessageType() {
            return "test";
        }
    }
}
