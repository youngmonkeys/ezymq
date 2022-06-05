package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicClient;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumers;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

public class EzyActiveTopicTest extends BaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {
        // given
        String name = RandomUtil.randomShortAlphabetString();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyActiveTopicClient client = mock(EzyActiveTopicClient.class);
        EzyActiveTopicServer server = mock(EzyActiveTopicServer.class);

        EzyActiveTopic<String> sut = EzyActiveTopic.builder()
            .name(name)
            .dataCodec(dataCodec)
            .client(client)
            .server(server)
            .build();
        AtomicReference<EzyActiveMessageHandler> messageHandlerRef = new AtomicReference<>();
        doAnswer(it -> {
            messageHandlerRef.set(
                it.getArgumentAt(0, EzyActiveMessageHandler.class)
            );
            return null;
        }).when(server).setMessageHandler(any(EzyActiveMessageHandler.class));

        EzyActiveProperties messageProperties = EzyActiveProperties.builder()
            .build();
        byte[] messageBody = RandomUtil.randomShortByteArray();
        doAnswer(it -> {
            messageHandlerRef.get().handle(
                messageProperties,
                messageBody
            );
            return null;
        }).when(server).start();

        EzyMQMessageConsumer<String> consumer1 = mock(EzyMQMessageConsumer.class);
        sut.addConsumer(consumer1);

        String command2 = RandomUtil.randomShortAlphabetString();
        EzyMQMessageConsumer<String> consumer2 = mock(EzyMQMessageConsumer.class);
        sut.addConsumer(command2, consumer2);

        String command3 = RandomUtil.randomShortAlphabetString();
        EzyMQMessageConsumer<String> consumer3 = mock(EzyMQMessageConsumer.class);
        sut.addConsumers(null);
        sut.addConsumers(
            EzyMapBuilder.mapBuilder()
                .put(command3, Collections.singletonList(consumer3))
                .build()
        );

        String data = RandomUtil.randomShortAlphabetString();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(dataCodec.serialize(data)).thenReturn(requestMessage);

        // when
        sut.publish(data);

        // then
        EzyMQMessageConsumers expectedConsumers = new EzyMQMessageConsumers();
        expectedConsumers.addConsumer("", consumer1);
        expectedConsumers.addConsumer(command2, consumer2);
        expectedConsumers.addConsumer(command3, consumer3);

        sut.close();
        verify(client, times(1)).close();
        verify(client, times(1)).publish(
            any(EzyActiveProperties.class),
            any(byte[].class)
        );
        verify(server, times(1)).start();
        verify(server, times(1)).close();
        verify(dataCodec, times(1)).serialize(data);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void publishFailedTest() throws Exception {
        // given
        String name = RandomUtil.randomShortAlphabetString();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyActiveTopicClient client = mock(EzyActiveTopicClient.class);

        EzyActiveTopic<InternalMessage> sut = EzyActiveTopic.builder()
            .name(name)
            .dataCodec(dataCodec)
            .client(client)
            .build();

        InternalMessage data = new InternalMessage();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(dataCodec.serialize(data)).thenReturn(requestMessage);

        RuntimeException exception = new RuntimeException("test");
        doThrow(exception)
            .when(client)
            .publish(
                any(EzyActiveProperties.class),
                any(byte[].class)
            );

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.publish(data)
        );

        // then
        Asserts.assertEqualsType(e, InternalServerErrorException.class);
        EzyMQMessageConsumer<InternalMessage> consumer1 = mock(EzyMQMessageConsumer.class);
        Asserts.assertThatThrows(() ->
            sut.addConsumer(consumer1)
        ).isEqualsType(IllegalStateException.class);
        sut.close();
        verify(client, times(1)).close();
        verify(client, times(1)).publish(
            any(EzyActiveProperties.class),
            any(byte[].class)
        );
        verify(dataCodec, times(1)).serialize(data);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void startConsumingTest() throws Exception {
        // given
        String name = RandomUtil.randomShortAlphabetString();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyActiveTopicServer server = mock(EzyActiveTopicServer.class);

        EzyActiveTopic<String> sut = EzyActiveTopic.builder()
            .name(name)
            .dataCodec(dataCodec)
            .server(server)
            .build();
        AtomicReference<EzyActiveMessageHandler> messageHandlerRef = new AtomicReference<>();
        doAnswer(it -> {
            messageHandlerRef.set(
                it.getArgumentAt(0, EzyActiveMessageHandler.class)
            );
            return null;
        }).when(server).setMessageHandler(any(EzyActiveMessageHandler.class));

        String command = RandomUtil.randomShortAlphabetString();
        EzyActiveProperties messageProperties = EzyActiveProperties.builder()
            .type(command)
            .build();
        byte[] messageBody = RandomUtil.randomShortByteArray();
        doAnswer(it -> {
            messageHandlerRef.get().handle(
                messageProperties,
                messageBody
            );
            return null;
        }).when(server).start();

        String data = RandomUtil.randomShortAlphabetString();
        byte[] requestMessage = RandomUtil.randomShortByteArray();
        when(dataCodec.serialize(data)).thenReturn(requestMessage);

        // when
        EzyMQMessageConsumer<String> consumer1 = mock(EzyMQMessageConsumer.class);
        sut.addConsumer(consumer1);

        // then
        Asserts.assertThatThrows(() ->
            sut.publish(data)
        ).isEqualsType(IllegalStateException.class);
        EzyMQMessageConsumers expectedConsumers = new EzyMQMessageConsumers();
        expectedConsumers.addConsumer("", consumer1);

        sut.close();
        verify(server, times(1)).start();
        verify(server, times(1)).close();
        verify(dataCodec, times(1)).deserializeTopicMessage(
            name,
            command,
            messageBody
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void startConsumingFailedTest() throws Exception {
        // given
        String name = RandomUtil.randomShortAlphabetString();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyActiveTopicServer server = mock(EzyActiveTopicServer.class);

        EzyActiveTopic<String> sut = EzyActiveTopic.builder()
            .name(name)
            .dataCodec(dataCodec)
            .server(server)
            .build();

        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(server).start();

        // when
        EzyMQMessageConsumer<String> consumer1 = mock(EzyMQMessageConsumer.class);
        Throwable e = Asserts.assertThrows(() ->
            sut.addConsumer(consumer1)
        );

        // then
        Asserts.assertEqualsType(e, IllegalStateException.class);
        EzyMQMessageConsumers expectedConsumers = new EzyMQMessageConsumers();
        expectedConsumers.addConsumer("", consumer1);

        sut.close();
        verify(server, times(1)).start();
        verify(server, times(1)).close();
    }

    private static class InternalMessage implements EzyMessageTypeFetcher {
        @Override
        public String getMessageType() {
            return "test";
        }
    }
}
