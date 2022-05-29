package com.tvd12.ezymq.kafka.testing.request;

import com.tvd12.ezyfox.message.annotation.EzyMessage;
import com.tvd12.ezyfox.message.annotation.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@EzyMessage
@AllArgsConstructor
@NoArgsConstructor
public class SumRequest {
    private int a;
    private int b;
}
