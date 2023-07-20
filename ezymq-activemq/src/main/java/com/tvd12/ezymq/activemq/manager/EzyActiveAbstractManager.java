package com.tvd12.ezymq.activemq.manager;

import com.tvd12.ezyfox.util.EzyLoggable;
import com.tvd12.ezymq.activemq.setting.EzyActiveEndpointSetting;
import lombok.AllArgsConstructor;

import javax.jms.Connection;
import javax.jms.Session;

@AllArgsConstructor
public class EzyActiveAbstractManager extends EzyLoggable {

    protected final Connection connection;

    public Session getSession(
        EzyActiveEndpointSetting setting
    ) throws Exception {
        Session session = setting.getSession();
        if (session == null) {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        return session;
    }
}
