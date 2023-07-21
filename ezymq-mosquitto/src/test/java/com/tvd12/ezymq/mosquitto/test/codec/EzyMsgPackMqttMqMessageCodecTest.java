package com.tvd12.ezymq.mosquitto.test.codec;

import com.tvd12.ezymq.mosquitto.codec.EzyMsgPackMqttMqMessageCodec;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.util.RandomUtil;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

public class EzyMsgPackMqttMqMessageCodecTest {

    private final EzyMsgPackMqttMqMessageCodec instance =
        new EzyMsgPackMqttMqMessageCodec();

    @Test
    public void test() {
        // given
        int id = RandomUtil.randomSmallInt();
        String type = RandomUtil.randomShortAlphabetString();
        String correlationId = RandomUtil.randomShortAlphabetString();
        Map<String, Object> headers = Collections.singletonMap(
            RandomUtil.randomShortAlphabetString(),
            RandomUtil.randomShortAlphabetString()
        );
        byte[] body = RandomUtil.randomShortByteArray();
        int qos = RandomUtil.randomInt(0, 1);
        boolean retained = RandomUtil.randomBoolean();
        EzyMqttMqMessage inputMqttMqMessage = EzyMqttMqMessage.builder()
            .id(id)
            .type(type)
            .correlationId(correlationId)
            .qos(qos)
            .retained(retained)
            .headers(headers)
            .body(body)
            .build();

        // when
        MqttMessage outputMessage = instance.encode(inputMqttMqMessage);
        EzyMqttMqMessage outputMqttMqMessage = instance.decode(outputMessage);

        // then
        Asserts.assertEquals(inputMqttMqMessage, outputMqttMqMessage, false);
    }

    @Test
    public void testWithNullHeader() {
        // given
        int id = RandomUtil.randomSmallInt();
        String type = RandomUtil.randomShortAlphabetString();
        String correlationId = RandomUtil.randomShortAlphabetString();
        byte[] body = RandomUtil.randomShortByteArray();
        int qos = RandomUtil.randomInt(0, 1);
        boolean retained = RandomUtil.randomBoolean();
        EzyMqttMqMessage inputMqttMqMessage = EzyMqttMqMessage.builder()
            .id(id)
            .type(type)
            .correlationId(correlationId)
            .qos(qos)
            .retained(retained)
            .body(body)
            .build();

        // when
        MqttMessage outputMessage = instance.encode(inputMqttMqMessage);
        EzyMqttMqMessage outputMqttMqMessage = instance.decode(outputMessage);

        // then
        Asserts.assertEquals(inputMqttMqMessage, outputMqttMqMessage, false);
    }
}
