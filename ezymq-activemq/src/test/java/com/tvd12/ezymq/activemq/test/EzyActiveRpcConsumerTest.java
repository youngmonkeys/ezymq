package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveRpcConsumer;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptor;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptors;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyActiveRpcConsumerTest extends BaseTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void handleFireTest() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyActiveRpcServer server = mock(EzyActiveRpcServer.class);

        EzyActiveRequestInterceptor interceptor = mock(EzyActiveRequestInterceptor.class);
        EzyActiveRequestInterceptors interceptors = new EzyActiveRequestInterceptors();
        interceptors.addInterceptor(interceptor);

        EzyActiveRequestHandler handler = mock(EzyActiveRequestHandler.class);
        EzyActiveRequestHandlers handlers = new EzyActiveRequestHandlers();
        String cmd = RandomUtil.randomShortAlphabetString();
        handlers.addHandler(cmd, handler);

        EzyActiveRpcConsumer sut = EzyActiveRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestInterceptors(interceptors)
            .requestHandlers(handlers)
            .build();

        EzyActiveProperties requestProperties = EzyActiveProperties.builder()
            .type(cmd)
            .build();
        byte[] requestBody = RandomUtil.randomShortByteArray();

        String requestEntity = RandomUtil.randomShortAlphabetString();
        when(dataCodec.deserialize(cmd, requestBody)).thenReturn(requestEntity);

        String responseEntity = RandomUtil.randomShortAlphabetString();
        when(handler.handle(requestEntity)).thenReturn(responseEntity);

        // when
        sut.handleFire(requestProperties, requestBody);

        // then
        verify(server, times(1)).setCallHandler(sut);
        verify(dataCodec, times(1)).deserialize(cmd, requestBody);
        verify(handler, times(1)).handle(requestEntity);
        verify(
            interceptor,
            times(1)
        ).preHandle(cmd, requestEntity);
        verify(
            interceptor,
            times(1)
        ).postHandle(cmd, requestEntity, responseEntity);

        sut.close();
        verify(server, times(1)).close();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void handleFireWithExceptionTest() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyActiveRpcServer server = mock(EzyActiveRpcServer.class);

        EzyActiveRequestInterceptor interceptor = mock(EzyActiveRequestInterceptor.class);
        EzyActiveRequestInterceptors interceptors = new EzyActiveRequestInterceptors();
        interceptors.addInterceptor(interceptor);

        EzyActiveRequestHandler handler = mock(EzyActiveRequestHandler.class);
        EzyActiveRequestHandlers handlers = new EzyActiveRequestHandlers();
        String cmd = RandomUtil.randomShortAlphabetString();
        handlers.addHandler(cmd, handler);

        EzyActiveRpcConsumer sut = EzyActiveRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestInterceptors(interceptors)
            .requestHandlers(handlers)
            .build();

        EzyActiveProperties requestProperties = EzyActiveProperties.builder()
            .type(cmd)
            .build();
        byte[] requestBody = RandomUtil.randomShortByteArray();

        String requestEntity = RandomUtil.randomShortAlphabetString();
        when(dataCodec.deserialize(cmd, requestBody)).thenReturn(requestEntity);

        RuntimeException exception = new RuntimeException("test");
        when(handler.handle(requestEntity)).thenThrow(exception);

        // when
        sut.handleFire(requestProperties, requestBody);

        // then
        verify(server, times(1)).setCallHandler(sut);
        verify(dataCodec, times(1)).deserialize(cmd, requestBody);
        verify(handler, times(1)).handle(requestEntity);
        verify(
            interceptor,
            times(1)
        ).preHandle(cmd, requestEntity);
        verify(
            interceptor,
            times(1)
        ).postHandle(cmd, requestEntity, exception);

        sut.close();
        verify(server, times(1)).close();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void handleCallTest() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyActiveRpcServer server = mock(EzyActiveRpcServer.class);

        EzyActiveRequestInterceptor interceptor = mock(EzyActiveRequestInterceptor.class);
        EzyActiveRequestInterceptors interceptors = new EzyActiveRequestInterceptors();
        interceptors.addInterceptor(interceptor);

        EzyActiveRequestHandler handler = mock(EzyActiveRequestHandler.class);
        EzyActiveRequestHandlers handlers = new EzyActiveRequestHandlers();
        String cmd = RandomUtil.randomShortAlphabetString();
        handlers.addHandler(cmd, handler);

        EzyActiveRpcConsumer sut = EzyActiveRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestInterceptors(interceptors)
            .requestHandlers(handlers)
            .build();

        EzyActiveProperties requestProperties = EzyActiveProperties.builder()
            .type(cmd)
            .build();
        byte[] requestBody = RandomUtil.randomShortByteArray();

        String requestEntity = RandomUtil.randomShortAlphabetString();
        when(dataCodec.deserialize(cmd, requestBody)).thenReturn(requestEntity);

        String responseEntity = RandomUtil.randomShortAlphabetString();
        when(handler.handle(requestEntity)).thenReturn(responseEntity);

        EzyActiveProperties.Builder replyPropertiesBuilder =
            EzyActiveProperties.builder();

        // when
        byte[] responseBytes = sut.handleCall(
            requestProperties,
            requestBody,
            replyPropertiesBuilder
        );

        // then
        verify(server, times(1)).setCallHandler(sut);
        verify(dataCodec, times(1)).deserialize(cmd, requestBody);
        verify(handler, times(1)).handle(requestEntity);
        verify(
            interceptor,
            times(1)
        ).preHandle(cmd, requestEntity);
        verify(
            interceptor,
            times(1)
        ).postHandle(cmd, requestEntity, responseEntity);
    }
}
