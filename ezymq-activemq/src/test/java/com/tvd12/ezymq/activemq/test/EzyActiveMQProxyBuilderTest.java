package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveMQProxyBuilder;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

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
        verify(connection, atLeast(1)).start();
        verify(
            connection, atLeast(1)
        ).createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
}
