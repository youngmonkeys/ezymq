package com.tvd12.ezymq.activemq.test.endpoint;

import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactoryBuilder;
import com.tvd12.ezymq.activemq.handler.EzyActiveExceptionListener;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;

import java.net.URI;
import java.util.Properties;

import static com.tvd12.ezymq.activemq.setting.EzyActiveSettings.*;
import static org.mockito.Mockito.mock;

public class EzyActiveConnectionFactoryBuilderTest extends BaseTest {

    @Test
    public void buildTest() {
        // given
        String uri = RandomUtil.randomShortAlphabetString();
        String username = RandomUtil.randomShortAlphabetString();
        String password = RandomUtil.randomShortAlphabetString();
        int maxThreadPoolSize = RandomUtil.randomInt(1, 3);
        ExceptionListener exceptionListener = mock(ExceptionListener.class);

        // when
        ConnectionFactory factory = new EzyActiveConnectionFactoryBuilder()
            .uri(uri)
            .username(username)
            .password(password)
            .maxThreadPoolSize(maxThreadPoolSize)
            .exceptionListener(exceptionListener)
            .build();

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "brokerURL"),
            URI.create(uri)
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "userName"),
            username
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "password"),
            password
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "maxThreadPoolSize"),
            maxThreadPoolSize
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "exceptionListener"),
            exceptionListener
        );
    }

    @Test
    public void buildDefault() {
        // given
        // when
        ConnectionFactory factory = new EzyActiveConnectionFactoryBuilder()
            .build();

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "brokerURL"),
            URI.create("failover://tcp://localhost:61616")
        );
        Asserts.assertNull(
            FieldUtil.getFieldValue(factory, "userName")
        );
        Asserts.assertNull(
            FieldUtil.getFieldValue(factory, "password")
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "exceptionListener").getClass(),
            EzyActiveExceptionListener.class
        );
    }

    @Test
    public void buildByPropertiesTest() {
        // given
        String uri = RandomUtil.randomShortAlphabetString();
        String username = RandomUtil.randomShortAlphabetString();
        String password = RandomUtil.randomShortAlphabetString();
        int maxThreadPoolSize = RandomUtil.randomInt(1, 3);
        int maxConnectionAttempts = RandomUtil.randomSmallInt() + 1;
        ExceptionListener exceptionListener = mock(ExceptionListener.class);

        Properties properties = new Properties();
        properties.setProperty(KEY_URI, uri);
        properties.setProperty(KEY_USERNAME, username);
        properties.setProperty(KEY_PASSWORD, password);
        properties.setProperty(KEY_MAX_THREAD_POOL_SIZE, String.valueOf(maxThreadPoolSize));

        // when
        ConnectionFactory factory = new EzyActiveConnectionFactoryBuilder()
            .properties(properties)
            .exceptionListener(exceptionListener)
            .maxConnectionAttempts(maxConnectionAttempts)
            .build();

        // then
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "brokerURL"),
            URI.create(uri)
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "userName"),
            username
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "password"),
            password
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "maxThreadPoolSize"),
            maxThreadPoolSize
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "exceptionListener"),
            exceptionListener
        );
        Asserts.assertEquals(
            FieldUtil.getFieldValue(factory, "maxConnectionAttempts"),
            maxConnectionAttempts
        );
    }
}
