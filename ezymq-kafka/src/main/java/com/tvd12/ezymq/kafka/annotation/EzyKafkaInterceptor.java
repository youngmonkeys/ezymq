package com.tvd12.ezymq.kafka.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * Indicates that a class handle a server event
 * 
 * @author tavandung12
 *
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE })
public @interface EzyKafkaInterceptor {
    
	/**
	 * command name
	 * 
	 * @return the event name
	 */
	public String value() default "";
	
    /**
     * command name
     * 
     * @return server event name
     */
	public String command() default "";
}
