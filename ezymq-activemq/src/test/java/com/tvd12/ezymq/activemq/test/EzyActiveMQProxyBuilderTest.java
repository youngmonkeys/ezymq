package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveMQProxyBuilder;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactory;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.reflect.MethodInvoker;
import org.testng.annotations.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class EzyActiveMQProxyBuilderTest extends BaseTest {

    @Test
    public void test() throws JMSException {
        // given
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(connection);

        Session session = mock(Session.class);
        when(
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        ).thenReturn(session);

        EzyActiveMQProxyBuilder sut = EzyActiveMQProxy.builder()
            .connectionFactory(connectionFactory)
            .settingsBuilder()
            .parent();

        // when
        sut.build();

        // then
        verify(connectionFactory, atLeast(1)).createConnection();
        verify(
            connection, atLeast(1)
        ).createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void preNewProxyTest() {
        // given
        EzyActiveSettings settings = mock(EzyActiveSettings.class);
        when(settings.getProperties()).thenReturn(new Properties());
        EzyActiveMQProxyBuilder instance = new EzyActiveMQProxyBuilder()
            .settings(settings);

        // when
        MethodInvoker.create()
            .object(instance)
            .method("preNewProxy")
            .invoke();

        // then
        ConnectionFactory connectionFactory = FieldUtil.getFieldValue(
            instance,
            "connectionFactory"
        );
        Asserts.assertEqualsType(connectionFactory, EzyActiveConnectionFactory.class);

        verify(settings, times(1)).getProperties();
        verifyNoMoreInteractions(settings);
    }

    @Test
    public void newProxyTest() throws Exception {
        // given
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        EzyActiveMQProxyBuilder instance = new EzyActiveMQProxyBuilder()
            .connectionFactory(connectionFactory);

        JMSException exception = new JMSException("test");
        when(connectionFactory.createConnection()).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() ->
            MethodInvoker.create()
                .object(instance)
                .method("newProxy")
                .invoke()
        );

        // then
        Asserts.assertEqualsType(e.getCause().getCause(), IllegalStateException.class);

        verify(connectionFactory, times(1)).createConnection();
        verifyNoMoreInteractions(connectionFactory);
    }
}
