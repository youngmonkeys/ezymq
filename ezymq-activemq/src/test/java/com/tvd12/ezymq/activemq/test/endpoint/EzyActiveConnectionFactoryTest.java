package com.tvd12.ezymq.activemq.test.endpoint;

import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactory;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.apache.activemq.command.Response;
import org.apache.activemq.transport.Transport;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EzyActiveConnectionFactoryTest extends BaseTest {

    @Test
    public void test() throws Exception {
        // given
        String uri = "failover://tcp://localhost:61616";
        Transport transportTest = mock(Transport.class);

        Response response = mock(Response.class);
        when(response.isException()).thenReturn(false);
        when(transportTest.request(any())).thenReturn(response);

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

    @Test
    public void newConnectionSuccessfullyButRetry() throws Exception {
        // given
        int maxConnectionAttempts = RandomUtil.randomInt(2, 10);
        AtomicInteger retryCount = new AtomicInteger();
        String uri = "failover://tcp://localhost:61616";
        Transport transportTest = mock(Transport.class);

        Response response = mock(Response.class);
        when(response.isException()).thenReturn(false);
        when(transportTest.request(any())).thenReturn(response);

        //noinspection ExternalizableWithoutPublicNoArgConstructor
        EzyActiveConnectionFactory sut = new EzyActiveConnectionFactory() {
            @Override
            protected Transport createTransport() {
                if (retryCount.incrementAndGet() == 1) {
                    throw new IllegalStateException("test");
                }
                return transportTest;
            }
        };
        sut.setBrokerURL(uri);
        sut.setMaxConnectionAttempts(maxConnectionAttempts);
        sut.setConnectionAttemptSleepTime(100);

        // when
        sut.createConnection();

        // then
        sut.close();
    }

    @Test
    public void newConnectionFailed() throws IOException {
        // given
        int maxConnectionAttempts = RandomUtil.randomInt(2, 10);
        String uri = "failover://tcp://localhost:61616";
        Transport transportTest = mock(Transport.class);

        Response response = mock(Response.class);
        when(response.isException()).thenReturn(false);
        when(transportTest.request(any())).thenReturn(response);

        RuntimeException exception = new RuntimeException("test");
        //noinspection ExternalizableWithoutPublicNoArgConstructor
        EzyActiveConnectionFactory sut = new EzyActiveConnectionFactory() {
            @Override
            protected Transport createTransport() {
                throw exception;
            }
        };
        sut.setBrokerURL(uri);
        sut.setMaxConnectionAttempts(maxConnectionAttempts);
        sut.setConnectionAttemptSleepTime(100);

        // when
        Throwable e = Asserts.assertThrows(sut::createConnection);

        // then
        Asserts.assertEquals(e.getCause(), exception);
    }
}
