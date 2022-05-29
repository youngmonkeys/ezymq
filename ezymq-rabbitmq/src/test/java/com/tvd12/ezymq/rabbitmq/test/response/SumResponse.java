package com.tvd12.ezymq.rabbitmq.test.response;

import com.tvd12.ezyfox.message.annotation.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@Message
@ToString
@AllArgsConstructor
public class SumResponse {
    private int sum;
}
