package com.tvd12.ezymq.rabbitmq.testing;

import java.util.concurrent.Executors;

import org.testng.annotations.Test;

import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactory;

public class EzyRabbitConnectionFactoryTest {
	
	@Test
	public void test() {
		EzyRabbitConnectionFactory factory = new EzyRabbitConnectionFactory();
		factory.setSharedExecutor(Executors.newSingleThreadExecutor());
		try {
			factory.newConnection();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		try {
			factory.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
