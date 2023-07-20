package com.tvd12.ezymq.activemq.test.endpoint;

import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicClient;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.*;

import static org.mockito.Mockito.*;

public class EzyActiveTopicClientTest extends BaseTest {

    @Test
    public void publishTest() throws Exception {
        // given
        Destination topic = mock(Destination.class);
        Session session = mock(Session.class);

        MessageProducer producer = mock(MessageProducer.class);
        when(session.createProducer(topic)).thenReturn(producer);

        EzyActiveTopicClient sut = EzyActiveTopicClient.builder()
            .session(session)
            .topic(topic)
            .build();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        // when
        EzyActiveProperties props = EzyActiveProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();
        sut.publish(props, message);

        // then
        verify(session, times(1)).createProducer(topic);
        verify(session, times(1)).createBytesMessage();
        verify(bytesMessage, times(1)).writeBytes(any());
        verify(producer, times(1)).send(bytesMessage);
        sut.close();
    }
}
