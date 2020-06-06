package com.tvd12.ezymq.activemq.test;

import javax.jms.Connection;
import javax.jms.Session;

import com.tvd12.ezyfox.builder.EzyArrayBuilder;
import com.tvd12.ezyfox.factory.EzyEntityFactory;
import com.tvd12.ezymq.activemq.EzyActiveRpcCaller;
import com.tvd12.ezymq.activemq.EzyActiveRpcHandler;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;

public class ActiveMQRpcAllRunner extends ActiveMQBaseTest {

	private EzyActiveRequestHandlers requestHandlers;
	
	public ActiveMQRpcAllRunner() {
		this.requestHandlers = new EzyActiveRequestHandlers();
		this.requestHandlers.addHandler("fibonaci", a -> {
			return (int)a + 3;
		});
	}
	
	public static void main(String[] args) throws Exception {
		EzyEntityFactory.create(EzyArrayBuilder.class);
		ActiveMQRpcAllRunner runner = new ActiveMQRpcAllRunner();
		runner.startServer();
		runner.sleep();
		runner.rpc();
	}
	
	@SuppressWarnings("resource")
	protected void startServer() throws Exception {
		try {
			System.out.println("thread-" + Thread.currentThread().getName() + ": start server");
			EzyActiveRpcServer server = newServer();
			EzyActiveRpcHandler handler = new EzyActiveRpcHandler(3, server, dataCodec, requestHandlers);
			handler.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void sleep() throws Exception {
		Thread.sleep(1000);
	}
	
	protected void asynRpc() {
		new Thread() {
			public void run() {
				try {
					rpc();
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
		}
		.start();
	}
	
	@SuppressWarnings("resource")
	protected void rpc() throws Exception {
		EzyActiveRpcClient client = newClient();
		EzyActiveRpcCaller caller = new EzyActiveRpcCaller(client, entityCodec);
		System.out.println("thread-" + Thread.currentThread().getName() + ": start rpc");
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 1000 ; ++i) {
			System.out.println("rabbit rpc start call: " + i);
			int result = caller.call("fibonaci", 100, int.class);
			System.out.println("i = " + i + ", result = " + result);
		}
		System.out.println("elapsed = " + (System.currentTimeMillis() - start));
	}
	
	protected EzyActiveRpcClient newClient() throws Exception {
		Connection connection = connectionFactory.createConnection();
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		return EzyActiveRpcClient.builder()
				.session(session)
				.defaultTimeout(300 * 1000)
				.requestQueueName("rpc-request-test-1")
				.replyQueueName("rpc-response-test-1")
				.build();
	}
	
	protected EzyActiveRpcServer newServer() throws Exception {
		Connection connection = connectionFactory.createConnection();
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		return EzyActiveRpcServer.builder()
				.session(session)
				.requestQueueName("rpc-request-test-1")
				.replyQueueName("rpc-response-test-1")
				.build();
	}
	
}
