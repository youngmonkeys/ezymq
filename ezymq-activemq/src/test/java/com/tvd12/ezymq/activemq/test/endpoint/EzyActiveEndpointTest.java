package com.tvd12.ezymq.activemq.test.endpoint;

import com.tvd12.ezymq.activemq.constant.EzyActiveDestinationType;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveEndpoint;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import static org.mockito.Mockito.*;

public class EzyActiveEndpointTest extends BaseTest {

    @Test
    public void createDestinationWithNameIsNullTest() {
        // given
        InternalBuilder builder = new InternalBuilder();

        // when
        Throwable e = Asserts.assertThrows(() ->
            builder.createDestination(
                EzyActiveDestinationType.QUEUE,
                null
            )
        );

        // then
        Asserts.assertEqualsType(e, NullPointerException.class);
    }

    @Test
    public void createDestinationFailedDueToCreateTopicTest() throws JMSException {
        // given
        Session session = mock(Session.class);
        RuntimeException exception = new RuntimeException("test");
        String topicName = RandomUtil.randomShortAlphabetString();
        when(session.createQueue(topicName)).thenThrow(exception);

        InternalBuilder builder = new InternalBuilder()
            .session(session);

        // when
        Throwable e = Asserts.assertThrows(() ->
            builder.createDestination(
                EzyActiveDestinationType.QUEUE,
                topicName
            )
        );

        // then
        Asserts.assertEqualsType(e, IllegalStateException.class);

        verify(session, times(1)).createQueue(topicName);
    }

    private static class InternalBuilder
        extends EzyActiveEndpoint.Builder<InternalBuilder> {

        @Override
        public Destination createDestination(
            EzyActiveDestinationType type,
            String name
        ) {
            return super.createDestination(type, name);
        }

        @Override
        public EzyActiveEndpoint build() {
            return new EzyActiveEndpoint(session);
        }
    }
}
