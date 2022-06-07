package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcConsumer;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandlers;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptor;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestInterceptors;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyRabbitRpcConsumerTest extends BaseTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void startLoopFailedTest() throws Exception {
        // given
        int threadPoolSize = 1;
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);

        EzyRabbitRequestHandler handler = mock(EzyRabbitRequestHandler.class);
        EzyRabbitRequestInterceptor interceptor = mock(EzyRabbitRequestInterceptor.class);

        EzyRabbitRequestHandlers handlers = new EzyRabbitRequestHandlers();
        String cmd = RandomUtil.randomShortAlphabetString();
        handlers.addHandler(cmd, handler);

        EzyRabbitRequestInterceptors interceptors = new EzyRabbitRequestInterceptors();
        interceptors.addInterceptor(interceptor);

        EzyRabbitRpcServer server = mock(EzyRabbitRpcServer.class);

        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(server).start();

        // when
        EzyRabbitRpcConsumer.builder()
            .threadPoolSize(threadPoolSize)
            .dataCodec(dataCodec)
            .requestHandlers(handlers)
            .requestInterceptors(interceptors)
            .server(server)
            .build();
        Thread.sleep(100);

        // then
        verify(server, times(1)).start();
    }
}
