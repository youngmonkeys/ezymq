package com.tvd12.ezymq.kafka.test.codec;

import com.tvd12.ezyfox.binding.EzyMarshaller;
import com.tvd12.ezyfox.binding.EzyUnmarshaller;
import com.tvd12.ezyfox.codec.EzyMessageDeserializer;
import com.tvd12.ezyfox.codec.EzyMessageSerializer;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.common.codec.EzyMQBytesDataCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaBytesDataCodec;
import com.tvd12.ezymq.kafka.codec.EzyKafkaDataCodec;
import com.tvd12.ezymq.kafka.test.request.MultiplyRequest;
import com.tvd12.ezymq.kafka.test.request.SumRequest;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.*;

public class EzyKafkaBytesDataCodecTest extends BaseTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void buildTest() {
        // given
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer messageDeserializer = mock(EzyMessageDeserializer.class);
        String topic1 = RandomUtil.randomShortAlphabetString();
        String topic2 = RandomUtil.randomShortAlphabetString();
        String topic3 = RandomUtil.randomShortAlphabetString();
        String command2 = RandomUtil.randomShortAlphabetString();
        String command3 = RandomUtil.randomShortAlphabetString();

        // given
        EzyKafkaDataCodec sut = EzyKafkaBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapMessageType(topic1, InternalMessage.class)
            .mapMessageType(topic2, command2, SumRequest.class)
            .mapMessageTypes(
                topic3,
                EzyMapBuilder.mapBuilder()
                    .put(command3, MultiplyRequest.class)
                    .toMap()
            )
            .build();

        // then
        Map<String, Map<String, Class>> messageTypesByTopic = FieldUtil.getFieldValue(
            sut,
            "messageTypesByTopic"
        );
        Asserts.assertEquals(
            messageTypesByTopic,
            EzyMapBuilder.mapBuilder()
                .put(
                    topic1,
                    EzyMapBuilder.mapBuilder()
                        .put("", InternalMessage.class)
                        .toMap()
                )
                .put(
                    topic2,
                    EzyMapBuilder.mapBuilder()
                        .put(command2, SumRequest.class)
                        .toMap()
                )
                .put(
                    topic3,
                    EzyMapBuilder.mapBuilder()
                        .put(command3, MultiplyRequest.class)
                        .toMap()
                )
                .toMap(),
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
        String topic = RandomUtil.randomShortAlphabetString();
        Map<String, Map<String, Class>> messageTypeMapByTopic = singletonMap(
            topic,
            singletonMap(command, InternalMessage.class)
        );
        EzyKafkaDataCodec sut = EzyKafkaBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .mapMessageTypes(messageTypeMapByTopic)
            .build();

        InternalMessage message = new InternalMessage();
        Object marshalledData = new Object();
        when(marshaller.marshal(message)).thenReturn(marshalledData);
        byte[] bytes = RandomUtil.randomShortByteArray();
        when(messageSerializer.serialize(marshalledData)).thenReturn(bytes);

        // given
        byte[] actual = sut.serialize(message);

        // then
        Asserts.assertEquals(actual, bytes);

        verify(marshaller, times(1)).marshal(message);
        verify(messageSerializer, times(1)).serialize(marshalledData);
    }

    @Test
    public void deserializeTextTest() {
        // given
        EzyMarshaller marshaller = mock(EzyMarshaller.class);
        EzyUnmarshaller unmarshaller = mock(EzyUnmarshaller.class);
        EzyMessageSerializer messageSerializer = mock(EzyMessageSerializer.class);
        EzyMessageDeserializer textMessageDeserializer = mock(EzyMessageDeserializer.class);
        String command = RandomUtil.randomShortAlphabetString();
        String topic = RandomUtil.randomShortAlphabetString();
        EzyKafkaDataCodec sut = EzyKafkaBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .textMessageDeserializer(textMessageDeserializer)
            .mapMessageTypes(
                topic,
                singletonMap(command, InternalMessage.class)
            )
            .build();

        byte[] bytes = RandomUtil.randomShortByteArray();
        Object data = new Object();
        when(textMessageDeserializer.deserialize(bytes)).thenReturn(data);
        InternalMessage message = new InternalMessage();
        when(unmarshaller.unmarshal(data, InternalMessage.class)).thenReturn(message);

        // given
        Object actual = sut.deserializeText(topic, command, bytes);

        // then
        Asserts.assertEquals(actual, message);

        verify(textMessageDeserializer, times(1)).deserialize(bytes);
        verify(unmarshaller, times(1)).unmarshal(data, InternalMessage.class);
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
        EzyKafkaDataCodec sut = EzyKafkaBytesDataCodec.builder()
            .marshaller(marshaller)
            .unmarshaller(unmarshaller)
            .messageSerializer(messageSerializer)
            .messageDeserializer(messageDeserializer)
            .build();

        byte[] bytes = RandomUtil.randomShortByteArray();
        Object data = new Object();
        when(messageDeserializer.deserialize(bytes)).thenReturn(data);
        InternalMessage message = new InternalMessage();
        when(unmarshaller.unmarshal(data, InternalMessage.class)).thenReturn(message);

        // given
        Throwable e = Asserts.assertThrows(() ->
            sut.deserializeText(topic, command, bytes)
        );

        // then
        Asserts.assertEqualsType(e, IllegalStateException.class);
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
            .mapRequestType(command, InternalMessage.class)
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
            .mapRequestType(command, InternalMessage.class)
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

    private static class InternalMessage {}
}
