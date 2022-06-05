package com.tvd12.ezymq.activemq.endpoint;

import com.tvd12.ezyfox.util.EzyCloseable;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;

import javax.jms.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.tvd12.ezyfox.util.EzyProcessor.processWithLogException;

public class EzyActiveConnectionFactory
    extends ActiveMQConnectionFactory
    implements EzyCloseable {

    protected final List<Connection> createdConnections =
        Collections.synchronizedList(new ArrayList<>());

    @Override
    protected ActiveMQConnection createActiveMQConnection(
        Transport transport,
        JMSStatsImpl stats
    ) throws Exception {
        ActiveMQConnection connection = super.createActiveMQConnection(
            transport,
            stats
        );
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
