package com.tvd12.ezymq.rabbitmq.test.entity;

import com.tvd12.ezyfox.annotation.EzyId;
import com.tvd12.ezyfox.binding.annotation.EzyObjectBinding;
import com.tvd12.ezyfox.message.annotation.EzyMessage;
import lombok.*;

@Setter
@Getter
@EzyMessage
@ToString
@EzyObjectBinding
@AllArgsConstructor
@NoArgsConstructor
public class HelloMessage {

    @EzyId
    private long messageId;
    private String content;

    public HelloMessage(long messageId) {
        this.messageId = messageId;
        this.content = "Message#" + messageId;
    }
}
