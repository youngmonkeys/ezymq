package com.tvd12.ezymq.activemq.endpoint;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezyfox.util.EzyThreads;
import lombok.Setter;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyActiveConnectionFactory
    extends ActiveMQConnectionFactory
    implements EzyCloseable {

    @Setter
    protected int maxConnectionAttempts;

    @Setter
    protected int connectionAttemptSleepTime = 3000;

    protected final List<Connection> createdConnections =
        Collections.synchronizedList(new ArrayList<>());

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Connection createConnection() throws JMSException {
        int retryCount = 0;
        Connection connection;
        while (true) {
            try {
                connection = super.createConnection();
                connection.start();
                break;
            } catch (Throwable e) {
                if (retryCount >= maxConnectionAttempts) {
                    throw e;
                }
                logger.error(
                    "can not connect to the broker, retry count: {}",
                    ++retryCount,
                    e
                );
                EzyThreads.sleep(connectionAttemptSleepTime);
            }
        }
        createdConnections.add(connection);
        return connection;
    }

    @Override
    public void close() {
        for (Connection connection : createdConnections) {
            processWithLogException(connection::close);
        }
    }
}
