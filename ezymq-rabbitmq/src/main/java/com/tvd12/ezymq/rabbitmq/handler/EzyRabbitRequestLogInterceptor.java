package com.tvd12.ezymq.rabbitmq.handler;

import com.tvd12.ezymq.common.handler.EzyMQRequestLogInterceptor;

public class EzyRabbitRequestLogInterceptor
    extends EzyMQRequestLogInterceptor
    implements EzyRabbitRequestInterceptor {}
