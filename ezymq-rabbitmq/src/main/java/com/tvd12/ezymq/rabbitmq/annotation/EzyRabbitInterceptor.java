package com.tvd12.ezymq.rabbitmq.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyRabbitInterceptor {

    int priority() default 0;
}
