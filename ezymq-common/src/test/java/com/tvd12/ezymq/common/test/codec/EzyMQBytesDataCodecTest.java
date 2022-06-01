package com.tvd12.ezymq.common.test.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import com.tvd12.ezymq.common.codec.EzyMQBytesDataCodec;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.*;

public class EzyMQBytesDataCodecTest extends BaseTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void buildTest() {
        // given
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        String command = RandomUtil.randomShortAlphabetString();
        String topic = RandomUtil.randomShortAlphabetString();

        // given
        EzyMQBytesDataCodec sut = EzyMQBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestType(command, InternalRequest.class)
            .mapTopicMessageType(topic, command, InternalMessage.class)
            .build();

        // then
        Map<String, Class> requestTypeByCommand = FieldUtil.getFieldValue(
            sut,
            "requestTypeByCommand"
        );
        Map<String, Map<String, Class>> messageTypeMapByTopic = FieldUtil.getFieldValue(
            sut,
            "messageTypeMapByTopic"
        );
        Asserts.assertEquals(
            requestTypeByCommand,
            singletonMap(
                command,
                InternalRequest.class
            ),
            false
        );
        Asserts.assertEquals(
            messageTypeMapByTopic,
            singletonMap(
                topic,
                singletonMap(command, InternalMessage.class)
            ),
            false
        );
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void serializeTest() {
        // given
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        String command = RandomUtil.randomShortAlphabetString();
        Map<String, Class> requestTypeMap = singletonMap(
            command,
            InternalRequest.class
        );
        String topic = RandomUtil.randomShortAlphabetString();
        Map<String, Map<String, Class>> messageTypeMapByTopic = singletonMap(
            topic,
            singletonMap(command, InternalMessage.class)
        );
        EzyMQBytesDataCodec sut = EzyMQBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestTypes(requestTypeMap)
            .mapTopicMessageTypes(messageTypeMapByTopic)
            .build();

        InternalRequest request = new InternalRequest();
        Object marshalledData = new Object();
        when(marshaller.marshal(request)).thenReturn(marshalledData);
        byte[] bytes = RandomUtil.randomShortByteArray();
        when(messageSerializer.serialize(marshalledData)).thenReturn(bytes);

        // given
        byte[] actual = sut.serialize(request);

        // then
        Asserts.assertEquals(actual, bytes);

        verify(marshaller, times(1)).marshal(request);
        verify(messageSerializer, times(1)).serialize(marshalledData);
    }

    @Test
    public void deserializeTest() {
        // given
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        String command = RandomUtil.randomShortAlphabetString();
        String topic = RandomUtil.randomShortAlphabetString();
        EzyMQBytesDataCodec sut = EzyMQBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestType(command, InternalRequest.class)
            .mapTopicMessageTypes(
                topic,
                singletonMap(command, InternalMessage.class)
            )
            .build();

        byte[] bytes = RandomUtil.randomShortByteArray();
        Object data = new Object();
        when(messageDeserializer.deserialize(bytes)).thenReturn(data);
        InternalRequest request = new InternalRequest();
        when(unmarshaller.unmarshal(data, InternalRequest.class)).thenReturn(request);

        // given
        Object actual = sut.deserialize(command, bytes);

        // then
        Asserts.assertEquals(actual, request);

        verify(messageDeserializer, times(1)).deserialize(bytes);
        verify(unmarshaller, times(1)).unmarshal(data, InternalRequest.class);
    }

    @Test
    public void deserializeFailedTest() {
        // given
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        String command = RandomUtil.randomShortAlphabetString();
        String topic = RandomUtil.randomShortAlphabetString();
        EzyMQBytesDataCodec sut = EzyMQBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapTopicMessageType(topic, command, InternalMessage.class)
            .build();

        byte[] bytes = RandomUtil.randomShortByteArray();
        Object data = new Object();
        when(messageDeserializer.deserialize(bytes)).thenReturn(data);
        InternalRequest request = new InternalRequest();
        when(unmarshaller.unmarshal(data, InternalRequest.class)).thenReturn(request);

        // given
        Throwable e = Asserts.assertThrows(() -> sut.deserialize(command, bytes));

        // then
        Asserts.assertEqualsType(e, IllegalArgumentException.class);

        verify(messageDeserializer, times(1)).deserialize(bytes);
        verify(unmarshaller, times(0)).unmarshal(data, InternalRequest.class);
    }

    @Test
    public void deserializeTopicMessageTest() {
        // given
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        String command = RandomUtil.randomShortAlphabetString();
        String topic = RandomUtil.randomShortAlphabetString();
        EzyMQBytesDataCodec sut = EzyMQBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestType(command, InternalRequest.class)
            .mapTopicMessageTypes(
                topic,
                singletonMap(command, InternalMessage.class)
            )
            .build();

        byte[] bytes = RandomUtil.randomShortByteArray();
        Object data = new Object();
        when(messageDeserializer.deserialize(bytes)).thenReturn(data);
        InternalMessage message = new InternalMessage();
        when(unmarshaller.unmarshal(data, InternalMessage.class)).thenReturn(message);

        // given
        Object actual = sut.deserializeTopicMessage(topic, command, bytes);

        // then
        Asserts.assertEquals(actual, message);

        verify(messageDeserializer, times(1)).deserialize(bytes);
        verify(unmarshaller, times(1)).unmarshal(data, InternalMessage.class);
    }

    @Test
    public void deserializeTopicMessageFailedTest() {
        // given
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        String command = RandomUtil.randomShortAlphabetString();
        String topic = RandomUtil.randomShortAlphabetString();
        EzyMQBytesDataCodec sut = EzyMQBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapRequestType(command, InternalRequest.class)
            .build();

        byte[] bytes = RandomUtil.randomShortByteArray();
        Object data = new Object();
        when(messageDeserializer.deserialize(bytes)).thenReturn(data);
        InternalMessage message = new InternalMessage();
        when(unmarshaller.unmarshal(data, InternalMessage.class)).thenReturn(message);

        // given
        Throwable e = Asserts.assertThrows(() ->
            sut.deserializeTopicMessage(topic, command, bytes)
        );

        // then
        Asserts.assertEqualsType(e, IllegalArgumentException.class);

        verify(messageDeserializer, times(1)).deserialize(bytes);
        verify(unmarshaller, times(0)).unmarshal(data, InternalMessage.class);
    }

    private static class InternalRequest {}

    private static class InternalMessage {}
}
