package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.codec.EzyMqttMqMessageCodec;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientFactory;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientFactoryBuilder;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings.*;
import static org.mockito.Mockito.mock;

public class EzyMosquittoConnectionFactoryBuilderTest extends BaseTest {

    @Test
    public void buildTest() {
        // given
        String uri = RandomUtil.randomShortAlphabetString();
        String username = RandomUtil.randomShortAlphabetString();
        String password = RandomUtil.randomShortAlphabetString();
        String clientIdPrefix = "test";
        int maxConnectionAttempts = RandomUtil.randomInt(1, 3);
        int connectionAttemptSleepTime = RandomUtil.randomInt(1, 3);
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);

        // when
        EzyMqttClientFactory factory = new EzyMqttClientFactoryBuilder()
            .serverUri(uri)
            .username(username)
            .password(password)
            .clientIdPrefix(clientIdPrefix)
            .maxConnectionAttempts(maxConnectionAttempts)
            .connectionAttemptSleepTime(connectionAttemptSleepTime)
            .mqttMqMessageCodec(mqttMqMessageCodec)
            .build();

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "serverUri"),
            uri
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "username"),
            username
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "password"),
            password
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "clientIdPrefix"),
            clientIdPrefix
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "maxConnectionAttempts"),
            maxConnectionAttempts
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "connectionAttemptSleepTime"),
            connectionAttemptSleepTime
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "mqttMqMessageCodec"),
            mqttMqMessageCodec
        );
    }

    @Test
    public void buildDefault() {
        // given
        // when
        EzyMqttClientFactory factory = new EzyMqttClientFactoryBuilder()
            .build();

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "serverUri"),
            "tcp://127.0.0.1:1883"
        );
        Asserts.assertNull(
            FieldUtil.getFieldValue(factory, "username")
        );
        Asserts.assertNull(
            FieldUtil.getFieldValue(factory, "password")
        );
    }

    @Test
    public void buildByPropertiesTest() {
        // given
        String uri = RandomUtil.randomShortAlphabetString();
        String username = RandomUtil.randomShortAlphabetString();
        String password = RandomUtil.randomShortAlphabetString();
        int maxConnectionAttempts = RandomUtil.randomSmallInt() + 1;

        Properties properties = new Properties();
        properties.setProperty(KEY_SERVER_URI, uri);
        properties.setProperty(KEY_USERNAME, username);
        properties.setProperty(KEY_PASSWORD, password);

        // when
        EzyMqttClientFactory factory = new EzyMqttClientFactoryBuilder()
            .properties(properties)
            .maxConnectionAttempts(maxConnectionAttempts)
            .build();

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "serverUri"),
            uri
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "username"),
            username
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "password"),
            password
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "maxConnectionAttempts"),
            maxConnectionAttempts
        );
    }
}
