package com.tvd12.ezymq.common.annotation;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class EzyConsumerAnnotationProperties {

    private String topic;
    private String command;
}
