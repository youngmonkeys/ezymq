package com.tvd12.ezymq.common.test.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyTestConsumer {

    String topic() default "";

    String command() default "";
}
