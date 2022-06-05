package com.tvd12.ezymq.activemq.test.manager;

import com.tvd12.ezymq.activemq.manager.EzyActiveAbstractManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveEndpointSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import static org.mockito.Mockito.*;

public class EzyActiveAbstractManagerTest extends BaseTest {

    @Test
    public void getSessionTest() throws Exception {
        // given
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        EzyActiveAbstractManager sut = new EzyActiveAbstractManager(
            connectionFactory
        );

        EzyActiveEndpointSetting setting = mock(EzyActiveEndpointSetting.class);
        Session session = mock(Session.class);
        when(setting.getSession()).thenReturn(session);

        // when
        Session actual = sut.getSession(setting);

        // then
        Asserts.assertEquals(actual, session);
        verify(setting, times(1)).getSession();
    }
}
