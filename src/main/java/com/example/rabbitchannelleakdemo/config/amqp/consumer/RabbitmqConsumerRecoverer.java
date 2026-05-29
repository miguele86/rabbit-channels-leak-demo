package com.example.rabbitchannelleakdemo.config.amqp.consumer;

import org.springframework.messaging.Message;

public interface RabbitmqConsumerRecoverer {
    boolean recoverMessage(Message<?> message, Throwable throwable);
}
