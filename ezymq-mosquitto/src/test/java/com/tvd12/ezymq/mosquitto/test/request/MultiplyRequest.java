package com.tvd12.ezymq.mosquitto.test.request;

import com.tvd12.ezyfox.message.annotation.EzyMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@EzyMessage
@AllArgsConstructor
@NoArgsConstructor
public class MultiplyRequest {
    private int a;
    private int b;
}
