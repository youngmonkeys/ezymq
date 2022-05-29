package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezyfox.tool.EzyBuilderCreator;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;

public class CreateBuilderTest {

    public static void main(String[] args) throws Exception {
        EzyBuilderCreator creator = new EzyBuilderCreator();
        System.out.println(creator.create(EzyRabbitTopic.class));
    }

}
