package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;

public class FiboRequest implements EzyMessageTypeFetcher {

    @Override
    public String getMessageType() {
        return "fibonacci";
    }
}
