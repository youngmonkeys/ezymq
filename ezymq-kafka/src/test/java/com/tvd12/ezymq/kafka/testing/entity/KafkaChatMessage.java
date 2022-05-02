package com.tvd12.ezymq.kafka.testing.entity;

import com.tvd12.ezyfox.annotation.EzyId;
import com.tvd12.ezyfox.binding.annotation.EzyObjectBinding;
import com.tvd12.ezyfox.message.annotation.EzyMessage;
import lombok.*;

@Setter
@Getter
@ToString
@EzyMessage
@NoArgsConstructor
@AllArgsConstructor
@EzyObjectBinding
public class KafkaChatMessage {
    @EzyId
    private long messageId;
    private String messageContent;
}
