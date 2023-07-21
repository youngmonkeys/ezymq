package com.tvd12.ezymq.mosquitto.test;

import com.tvd12.ezyfox.exception.BadRequestException;
import com.tvd12.ezyfox.util.EzyThreads;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.constant.EzyRpcErrorCodes;
import com.tvd12.ezymq.mosquitto.EzyMosquittoRpcConsumer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcServer;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandler;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestHandlers;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestInterceptor;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRequestInterceptors;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static com.tvd12.ezymq.common.util.EzyRpcExceptionTranslators.exceptionToResponseHeaders;
import static org.mockito.Mockito.*;

public class EzyMosquittoRpcConsumerTest extends BaseTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void handleFireTest() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyMosquittoRpcServer server = mock(EzyMosquittoRpcServer.class);

        EzyMosquittoRequestInterceptor interceptor = mock(EzyMosquittoRequestInterceptor.class);
        EzyMosquittoRequestInterceptors interceptors = new EzyMosquittoRequestInterceptors();
        interceptors.addInterceptor(interceptor);

        EzyMosquittoRequestHandler handler = mock(EzyMosquittoRequestHandler.class);
        EzyMosquittoRequestHandlers handlers = new EzyMosquittoRequestHandlers();
        String cmd = RandomUtil.randomShortAlphabetString();
        handlers.addHandler(cmd, handler);

        EzyMosquittoRpcConsumer sut = EzyMosquittoRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestInterceptors(interceptors)
            .requestHandlers(handlers)
            .build();

        EzyMosquittoProperties requestProperties = EzyMosquittoProperties.builder()
            .messageType(cmd)
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
        EzyMosquittoRpcServer server = mock(EzyMosquittoRpcServer.class);

        EzyMosquittoRequestInterceptor interceptor = mock(EzyMosquittoRequestInterceptor.class);
        EzyMosquittoRequestInterceptors interceptors = new EzyMosquittoRequestInterceptors();
        interceptors.addInterceptor(interceptor);

        EzyMosquittoRequestHandler handler = mock(EzyMosquittoRequestHandler.class);
        EzyMosquittoRequestHandlers handlers = new EzyMosquittoRequestHandlers();
        String cmd = RandomUtil.randomShortAlphabetString();
        handlers.addHandler(cmd, handler);

        EzyMosquittoRpcConsumer sut = EzyMosquittoRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestInterceptors(interceptors)
            .requestHandlers(handlers)
            .build();

        EzyMosquittoProperties requestProperties = EzyMosquittoProperties.builder()
            .messageType(cmd)
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
        EzyMosquittoRpcServer server = mock(EzyMosquittoRpcServer.class);

        EzyMosquittoRequestInterceptor interceptor = mock(EzyMosquittoRequestInterceptor.class);
        EzyMosquittoRequestInterceptors interceptors = new EzyMosquittoRequestInterceptors();
        interceptors.addInterceptor(interceptor);

        EzyMosquittoRequestHandler handler = mock(EzyMosquittoRequestHandler.class);
        EzyMosquittoRequestHandlers handlers = new EzyMosquittoRequestHandlers();
        String cmd = RandomUtil.randomShortAlphabetString();
        handlers.addHandler(cmd, handler);

        EzyMosquittoRpcConsumer sut = EzyMosquittoRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestInterceptors(interceptors)
            .requestHandlers(handlers)
            .build();

        EzyMosquittoProperties requestProperties = EzyMosquittoProperties.builder()
            .messageType(cmd)
            .build();
        byte[] requestBody = RandomUtil.randomShortByteArray();

        String requestEntity = RandomUtil.randomShortAlphabetString();
        when(dataCodec.deserialize(cmd, requestBody)).thenReturn(requestEntity);

        String responseEntity = RandomUtil.randomShortAlphabetString();
        when(handler.handle(requestEntity)).thenReturn(responseEntity);

        byte[] responseBody = RandomUtil.randomShortByteArray();
        when(dataCodec.serialize(responseEntity)).thenReturn(responseBody);

        EzyMosquittoProperties.Builder replyPropertiesBuilder =
            EzyMosquittoProperties.builder();

        // when
        byte[] responseBytes = sut.handleCall(
            requestProperties,
            requestBody,
            replyPropertiesBuilder
        );

        // then
        Asserts.assertEquals(
            responseBytes,
            responseBody
        );
        verify(server, times(1)).setCallHandler(sut);
        verify(dataCodec, times(1)).deserialize(cmd, requestBody);
        verify(dataCodec, times(1)).serialize(responseEntity);
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void handleCallWithBadRequestExceptionTest() throws Exception {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyMosquittoRpcServer server = mock(EzyMosquittoRpcServer.class);

        EzyMosquittoRequestInterceptor interceptor = mock(EzyMosquittoRequestInterceptor.class);
        EzyMosquittoRequestInterceptors interceptors = new EzyMosquittoRequestInterceptors();
        interceptors.addInterceptor(interceptor);

        EzyMosquittoRequestHandler handler = mock(EzyMosquittoRequestHandler.class);
        EzyMosquittoRequestHandlers handlers = new EzyMosquittoRequestHandlers();
        String cmd = RandomUtil.randomShortAlphabetString();
        handlers.addHandler(cmd, handler);

        EzyMosquittoRpcConsumer sut = EzyMosquittoRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestInterceptors(interceptors)
            .requestHandlers(handlers)
            .threadPoolSize(0)
            .build();

        EzyMosquittoProperties requestProperties = EzyMosquittoProperties.builder()
            .messageType(cmd)
            .build();
        byte[] requestBody = RandomUtil.randomShortByteArray();

        String requestEntity = RandomUtil.randomShortAlphabetString();
        when(dataCodec.deserialize(cmd, requestBody)).thenReturn(requestEntity);

        BadRequestException exception = new BadRequestException(
            EzyRpcErrorCodes.INVALID_ARGUMENT,
            "test"
        );
        when(handler.handle(requestEntity)).thenThrow(exception);

        EzyMosquittoProperties.Builder replyPropertiesBuilder =
            EzyMosquittoProperties.builder();

        // when
        byte[] responseBytes = sut.handleCall(
            requestProperties,
            requestBody,
            replyPropertiesBuilder
        );

        // then
        Asserts.assertEquals(
            responseBytes,
            new byte[0]
        );
        Asserts.assertEquals(
            replyPropertiesBuilder,
            EzyMosquittoProperties
                .builder()
                .headers(exceptionToResponseHeaders(exception))
        );
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
    }

    @Test
    public void buildWithDefaultInterceptors() {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyMosquittoRpcServer server = mock(EzyMosquittoRpcServer.class);
        EzyMosquittoRequestHandlers handlers = new EzyMosquittoRequestHandlers();

        // when
        EzyMosquittoRpcConsumer sut = EzyMosquittoRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestHandlers(handlers)
            .threadPoolSize(1)
            .build();

        // then
        sut.close();
    }

    @Test
    public void startLoopHasExceptionTest() {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyMosquittoRpcServer server = mock(EzyMosquittoRpcServer.class);
        EzyMosquittoRequestHandlers handlers = new EzyMosquittoRequestHandlers();

        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(server).start();

        // when
        EzyMosquittoRpcConsumer sut = EzyMosquittoRpcConsumer.builder()
            .dataCodec(dataCodec)
            .server(server)
            .requestHandlers(handlers)
            .threadPoolSize(1)
            .build();
        EzyThreads.sleep(100);

        // then
        verify(server, times(1)).setCallHandler(sut);
        verify(server, times(1)).start();
        verifyNoMoreInteractions(server);
        sut.close();
    }
}
