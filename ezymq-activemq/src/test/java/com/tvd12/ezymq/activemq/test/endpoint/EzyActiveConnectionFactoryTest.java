package com.tvd12.ezymq.activemq.test.endpoint;

import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactory;
import com.tvd12.test.base.BaseTest;
import org.apache.activemq.transport.Transport;
import org.testng.annotations.Test;

import javax.jms.JMSException;

import static org.mockito.Mockito.mock;

public class EzyActiveConnectionFactoryTest extends BaseTest {

    @Test
    public void test() throws JMSException {
        // given
        String uri = "failover://tcp://localhost:61616";
        Transport transportTest = mock(Transport.class);
        //noinspection ExternalizableWithoutPublicNoArgConstructor
        EzyActiveConnectionFactory sut = new EzyActiveConnectionFactory() {
            @Override
            protected Transport createTransport() {
                return transportTest;
            }
        };
        sut.setBrokerURL(uri);

        // when
        sut.createConnection();

        // then
        sut.close();
    }
}
