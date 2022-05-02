package com.tvd12.ezymq.activemq.manager;

import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.activemq.EzyActiveRpcHandler;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcHandlerSetting;

import javax.jms.ConnectionFactory;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;

public class EzyActiveRpcHandlerManager
    extends EzyActiveAbstractManager implements EzyCloseable {

    protected final EzyActiveDataCodec dataCodec;
    protected final Map<String, EzyActiveRpcHandler> rpcHandlers;
    protected final Map<String, EzyActiveRpcHandlerSetting> rpcHandlerSettings;

    public EzyActiveRpcHandlerManager(
        EzyActiveDataCodec dataCodec,
        ConnectionFactory connectionFactory,
        Map<String, EzyActiveRpcHandlerSetting> rpcHandlerSettings
    ) {
        super(connectionFactory);
        this.dataCodec = dataCodec;
        this.rpcHandlerSettings = rpcHandlerSettings;
        this.rpcHandlers = createRpcCallers();
    }

    public EzyActiveRpcHandler getRpcHandler(String name) {
        EzyActiveRpcHandler handler = rpcHandlers.get(name);
        if (handler == null) {
            throw new IllegalArgumentException("has no rpc handler with name: " + name);
        }
        return handler;
    }

    protected Map<String, EzyActiveRpcHandler> createRpcCallers() {
        Map<String, EzyActiveRpcHandler> map = new HashMap<>();
        for (String name : rpcHandlerSettings.keySet()) {
            EzyActiveRpcHandlerSetting setting = rpcHandlerSettings.get(name);
            map.put(name, createRpcHandler(name, setting));
        }
        return map;
    }

    protected EzyActiveRpcHandler createRpcHandler(
        String name,
        EzyActiveRpcHandlerSetting setting
    ) {
        try {
            return createRpcHandler(setting);
        } catch (Exception e) {
            throw new IllegalStateException("can't create handler: " + name, e);
        }
    }

    protected EzyActiveRpcHandler createRpcHandler(
        EzyActiveRpcHandlerSetting setting
    ) throws Exception {
        Session session = getSession(setting);
        EzyActiveRpcServer client = EzyActiveRpcServer.builder()
            .session(session)
            .threadPoolSize(setting.getThreadPoolSize())
            .requestQueueName(setting.getRequestQueueName())
            .requestQueue(setting.getRequestQueue())
            .replyQueueName(setting.getReplyQueueName())
            .replyQueue(setting.getReplyQueue())
            .build();
        EzyActiveRpcHandler handler = EzyActiveRpcHandler.builder()
            .dataCodec(dataCodec)
            .actionInterceptor(setting.getActionInterceptor())
            .requestHandlers(setting.getRequestHandlers())
            .server(client).build();
        handler.start();
        return handler;
    }

    public void close() {
        for (EzyActiveRpcHandler handler : rpcHandlers.values()) {
            handler.close();
        }
    }
}
