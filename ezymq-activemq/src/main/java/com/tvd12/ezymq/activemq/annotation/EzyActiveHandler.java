package com.tvd12.ezymq.activemq.annotation;

import java.lang.annotation.*;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EzyActiveHandler {

    String value() default "";

    String command() default "";
}
