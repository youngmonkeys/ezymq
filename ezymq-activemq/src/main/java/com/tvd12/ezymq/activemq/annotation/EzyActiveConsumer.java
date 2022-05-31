package com.tvd12.ezymq.activemq.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyActiveConsumer {

    String topic() default "";

    String command() default "";
}
