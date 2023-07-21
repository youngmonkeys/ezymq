package com.tvd12.ezymq.mosquitto.test;

import com.tvd12.ezyfox.exception.InternalServerErrorException;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumer;
import com.tvd12.ezymq.common.handler.EzyMQMessageConsumers;
import com.tvd12.ezymq.mosquitto.EzyMosquittoTopic;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoTopicClient;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoTopicServer;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoMessageHandler;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

public class EzyMosquittoTopicTest extends BaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws Exception {
        // given
        String name = RandomUtil.randomShortAlphabetString();
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        EzyMosquittoTopicClient client = mock(EzyMosquittoTopicClient.class);
        EzyMosquittoTopicServer server = mock(EzyMosquittoTopicServer.class);

        EzyMosquittoTopic<String> sut = EzyMosquittoTopic.builder()
            .name(name)
            .dataCodec(dataCodec)
            .client(client)
            .server(server)
            .build();
        AtomicReference<EzyMosquittoMessageHandler> messageHandlerRef = new AtomicReference<>();
        doAnswer(it -> {
            messageHandlerRef.set(
                it.getArgumentAt(0, EzyMosquittoMessageHandler.class)
            );
            return null;
        }).when(server).setMessageHandler(any(EzyMosquittoMessageHandler.class));

        EzyMosquittoProperties messageProperties = EzyMosquittoProperties.builder()
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
            any(EzyMosquittoProperties.class),
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
        EzyMosquittoTopicClient client = mock(EzyMosquittoTopicClient.class);

        EzyMosquittoTopic<InternalMessage> sut = EzyMosquittoTopic.builder()
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
                any(EzyMosquittoProperties.class),
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
            any(EzyMosquittoProperties.class),
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
        EzyMosquittoTopicServer server = mock(EzyMosquittoTopicServer.class);

        EzyMosquittoTopic<String> sut = EzyMosquittoTopic.builder()
            .name(name)
            .dataCodec(dataCodec)
            .server(server)
            .build();
        AtomicReference<EzyMosquittoMessageHandler> messageHandlerRef = new AtomicReference<>();
        doAnswer(it -> {
            messageHandlerRef.set(
                it.getArgumentAt(0, EzyMosquittoMessageHandler.class)
            );
            return null;
        }).when(server).setMessageHandler(any(EzyMosquittoMessageHandler.class));

        String command = RandomUtil.randomShortAlphabetString();
        EzyMosquittoProperties messageProperties = EzyMosquittoProperties.builder()
            .messageType(command)
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
        EzyMosquittoTopicServer server = mock(EzyMosquittoTopicServer.class);

        EzyMosquittoTopic<String> sut = EzyMosquittoTopic.builder()
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
