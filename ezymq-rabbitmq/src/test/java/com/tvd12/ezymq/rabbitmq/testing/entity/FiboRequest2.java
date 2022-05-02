package com.tvd12.ezymq.rabbitmq.testing.entity;

import com.tvd12.ezyfox.binding.annotation.EzyObjectBinding;
import com.tvd12.ezyfox.message.EzyMessageTypeFetcher;

@EzyObjectBinding
public class FiboRequest2 implements EzyMessageTypeFetcher {

    @Override
    public String getMessageType() {
        return "fibonacci2";
    }
}
