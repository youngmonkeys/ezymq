package com.tvd12.ezymq.kafka.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyKafkaInterceptor {

    int priority() default 0;
}
