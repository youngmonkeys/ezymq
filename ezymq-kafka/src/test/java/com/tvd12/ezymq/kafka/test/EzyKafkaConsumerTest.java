package com.tvd12.ezymq.kafka.test;

import com.tvd12.ezymq.kafka.EzyKafkaConsumer;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.endpoint.EzyKafkaServer;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandler;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageHandlers;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptor;
import com.tvd12.ezymq.kafka.handler.EzyKafkaMessageInterceptors;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyKafkaConsumerTest extends BaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void handleRecordWithHandlerIsNotNullTest() throws Exception {
        // given
        EzyKafkaServer server = mock(EzyKafkaServer.class);
        EzyKafkaDataCodec dataCodec = mock(EzyKafkaDataCodec.class);

        String cmd = RandomUtil.randomShortAlphabetString();
        EzyKafkaMessageHandler<String> messageHandler = mock(EzyKafkaMessageHandler.class);
        EzyKafkaMessageHandlers messageHandlers = new EzyKafkaMessageHandlers();
        messageHandlers.addHandler(cmd, messageHandler);

        EzyKafkaMessageInterceptor messageInterceptor = mock(EzyKafkaMessageInterceptor.class);
        EzyKafkaMessageInterceptors messageInterceptors = new EzyKafkaMessageInterceptors();
        messageInterceptors.addInterceptor(messageInterceptor);

        EzyKafkaConsumer sut = EzyKafkaConsumer.builder()
            .server(server)
            .dataCodec(dataCodec)
            .messageHandlers(messageHandlers)
            .messageInterceptors(messageInterceptors)
            .build();

        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        String topic = RandomUtil.randomShortAlphabetString();
        when(record.topic()).thenReturn(topic);

        when(record.key()).thenReturn(cmd.getBytes());

        Headers headers = mock(Headers.class);
        when(record.headers()).thenReturn(headers);

        Header header = mock(Header.class);
        when(headers.lastHeader("c")).thenReturn(header);

        byte[] binaryType = FieldUtil.getStaticFieldValue(
            EzyKafkaConsumer.class,
            "BINARY_TYPE"
        );
        when(header.value()).thenReturn(binaryType);

        byte[] requestBody = RandomUtil.randomShortByteArray();
        when(record.value()).thenReturn(requestBody);

        String message = RandomUtil.randomShortAlphabetString();
        when(dataCodec.deserialize(topic, cmd, requestBody)).thenReturn(message);

        when(messageHandler.handle(message)).thenReturn(Boolean.TRUE);

        // when
        sut.handleRecord(record);

        // then
        verify(server, times(1)).setRecordsHandler(sut);
        verify(record, times(1)).topic();
        verify(record, times(1)).key();
        verify(record, times(1)).headers();
        verify(headers, times(1)).lastHeader("c");
        verify(header, times(1)).value();
        verify(record, times(1)).value();
        verify(
            dataCodec,
            times(1)
        ).deserialize(topic, cmd, requestBody);
        verify(
            dataCodec,
            times(0)
        ).deserializeText(any(), any(), any());
        verify(
            messageInterceptor,
            times(1)
        ).preHandle(topic, cmd, message);
        verify(
            messageInterceptor,
            times(1)
        ).postHandle(topic, cmd, message, Boolean.TRUE);
        verify(
            messageHandler,
            times(1)
        ).handle(message);

        sut.close();
        verify(server, times(1)).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void handleRecordWithHandlerIsNotNullButTextTest() throws Exception {
        // given
        EzyKafkaServer server = mock(EzyKafkaServer.class);
        EzyKafkaDataCodec dataCodec = mock(EzyKafkaDataCodec.class);

        String cmd = RandomUtil.randomShortAlphabetString();
        EzyKafkaMessageHandler<String> messageHandler = mock(EzyKafkaMessageHandler.class);
        EzyKafkaMessageHandlers messageHandlers = new EzyKafkaMessageHandlers();
        messageHandlers.addHandler(cmd, messageHandler);

        EzyKafkaMessageInterceptor messageInterceptor = mock(EzyKafkaMessageInterceptor.class);
        EzyKafkaMessageInterceptors messageInterceptors = new EzyKafkaMessageInterceptors();
        messageInterceptors.addInterceptor(messageInterceptor);

        EzyKafkaConsumer sut = EzyKafkaConsumer.builder()
            .server(server)
            .dataCodec(dataCodec)
            .messageHandlers(messageHandlers)
            .messageInterceptors(messageInterceptors)
            .build();

        ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
        String topic = RandomUtil.randomShortAlphabetString();
        when(record.topic()).thenReturn(topic);

        when(record.key()).thenReturn(cmd);

        Headers headers = mock(Headers.class);
        when(record.headers()).thenReturn(headers);

        Header header = mock(Header.class);
        when(headers.lastHeader("c")).thenReturn(header);

        String requestBody = RandomUtil.randomShortAlphabetString();
        when(record.value()).thenReturn(requestBody);

        String message = RandomUtil.randomShortAlphabetString();
        when(
            dataCodec.deserializeText(topic, cmd, requestBody.getBytes())
        ).thenReturn(message);

        when(messageHandler.handle(message)).thenReturn(Boolean.TRUE);

        // when
        sut.handleRecord(record);

        // then
        verify(server, times(1)).setRecordsHandler(sut);
        verify(record, times(1)).topic();
        verify(record, times(1)).key();
        verify(record, times(1)).headers();
        verify(headers, times(1)).lastHeader("c");
        verify(header, times(1)).value();
        verify(record, times(1)).value();
        verify(
            dataCodec,
            times(0)
        ).deserialize(any(), any(), any());
        verify(
            dataCodec,
            times(1)
        ).deserializeText(topic, cmd, requestBody.getBytes());
        verify(
            messageInterceptor,
            times(1)
        ).preHandle(topic, cmd, message);
        verify(
            messageInterceptor,
            times(1)
        ).postHandle(topic, cmd, message, Boolean.TRUE);
        verify(
            messageHandler,
            times(1)
        ).handle(message);

        sut.close();
        verify(server, times(1)).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void handleRecordWithHandlerIsNullTest() throws Exception {
        // given
        EzyKafkaServer server = mock(EzyKafkaServer.class);
        EzyKafkaDataCodec dataCodec = mock(EzyKafkaDataCodec.class);

        String cmd = "";
        EzyKafkaMessageHandler<String> messageHandler = mock(EzyKafkaMessageHandler.class);
        EzyKafkaMessageHandlers messageHandlers = new EzyKafkaMessageHandlers();
        messageHandlers.addHandler(cmd, messageHandler);

        EzyKafkaMessageInterceptor messageInterceptor = mock(EzyKafkaMessageInterceptor.class);
        EzyKafkaMessageInterceptors messageInterceptors = new EzyKafkaMessageInterceptors();
        messageInterceptors.addInterceptor(messageInterceptor);

        EzyKafkaConsumer sut = EzyKafkaConsumer.builder()
            .server(server)
            .dataCodec(dataCodec)
            .messageHandlers(messageHandlers)
            .messageInterceptors(messageInterceptors)
            .build();

        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        String topic = RandomUtil.randomShortAlphabetString();
        when(record.topic()).thenReturn(topic);

        Headers headers = mock(Headers.class);
        when(record.headers()).thenReturn(headers);

        byte[] requestBody = RandomUtil.randomShortByteArray();
        when(record.value()).thenReturn(requestBody);

        String message = RandomUtil.randomShortAlphabetString();
        when(dataCodec.deserialize(topic, cmd, requestBody)).thenReturn(message);

        when(messageHandler.handle(message)).thenReturn(Boolean.TRUE);

        // when
        sut.handleRecord(record);

        // then
        verify(server, times(1)).setRecordsHandler(sut);
        verify(record, times(1)).topic();
        verify(record, times(1)).key();
        verify(record, times(1)).headers();
        verify(headers, times(1)).lastHeader("c");
        verify(record, times(1)).value();
        verify(
            dataCodec,
            times(1)
        ).deserialize(topic, cmd, requestBody);
        verify(
            dataCodec,
            times(0)
        ).deserializeText(any(), any(), any());
        verify(
            messageInterceptor,
            times(1)
        ).preHandle(topic, cmd, message);
        verify(
            messageInterceptor,
            times(1)
        ).postHandle(topic, cmd, message, Boolean.TRUE);
        verify(
            messageHandler,
            times(1)
        ).handle(message);

        sut.close();
        verify(server, times(1)).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void handleRecordFailedDueToCodec() {
        // given
        EzyKafkaServer server = mock(EzyKafkaServer.class);
        EzyKafkaDataCodec dataCodec = mock(EzyKafkaDataCodec.class);

        String cmd = RandomUtil.randomShortAlphabetString();
        EzyKafkaMessageHandler<String> messageHandler = mock(EzyKafkaMessageHandler.class);
        EzyKafkaMessageHandlers messageHandlers = new EzyKafkaMessageHandlers();
        messageHandlers.addHandler(cmd, messageHandler);

        EzyKafkaMessageInterceptor messageInterceptor = mock(EzyKafkaMessageInterceptor.class);
        EzyKafkaMessageInterceptors messageInterceptors = new EzyKafkaMessageInterceptors();
        messageInterceptors.addInterceptor(messageInterceptor);

        EzyKafkaConsumer sut = EzyKafkaConsumer.builder()
            .server(server)
            .dataCodec(dataCodec)
            .messageHandlers(messageHandlers)
            .messageInterceptors(messageInterceptors)
            .build();

        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        String topic = RandomUtil.randomShortAlphabetString();
        when(record.topic()).thenReturn(topic);

        when(record.key()).thenReturn(cmd.getBytes());

        Headers headers = mock(Headers.class);
        when(record.headers()).thenReturn(headers);

        Header header = mock(Header.class);
        when(headers.lastHeader("c")).thenReturn(header);

        byte[] binaryType = FieldUtil.getStaticFieldValue(
            EzyKafkaConsumer.class,
            "BINARY_TYPE"
        );
        when(header.value()).thenReturn(binaryType);

        byte[] requestBody = RandomUtil.randomShortByteArray();
        when(record.value()).thenReturn(requestBody);

        RuntimeException exception = new RuntimeException("test");
        when(
            dataCodec.deserialize(topic, cmd, requestBody)
        ).thenThrow(exception);

        // when
        sut.handleRecord(record);

        // then
        verify(server, times(1)).setRecordsHandler(sut);
        verify(record, times(1)).topic();
        verify(record, times(1)).key();
        verify(record, times(1)).headers();
        verify(headers, times(1)).lastHeader("c");
        verify(header, times(1)).value();
        verify(record, times(1)).value();
        verify(
            dataCodec,
            times(1)
        ).deserialize(topic, cmd, requestBody);
        verify(
            dataCodec,
            times(0)
        ).deserializeText(any(), any(), any());
        verify(
            messageInterceptor,
            times(1)
        ).postHandle(topic, cmd, null, exception);

        sut.close();
        verify(server, times(1)).close();
    }
}
