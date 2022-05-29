package com.tvd12.ezymq.activemq.test.request;

import com.tvd12.ezyfox.message.annotation.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Message
@AllArgsConstructor
@NoArgsConstructor
public class SumRequest {
    private int a;
    private int b;
}
