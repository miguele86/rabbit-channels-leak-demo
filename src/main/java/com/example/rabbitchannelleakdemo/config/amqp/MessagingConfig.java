package com.example.rabbitchannelleakdemo.config.amqp;

import com.example.rabbitchannelleakdemo.config.amqp.consumer.RabbitmqConsumer;
import com.example.rabbitchannelleakdemo.config.amqp.consumer.RabbitmqConsumerRecoverer;
import com.example.rabbitchannelleakdemo.model.Payload;
import com.example.rabbitchannelleakdemo.service.MessagePublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Configuration
public class MessagingConfig {

    private final MessagePublisher messagePublisher;

    public MessagingConfig(MessagePublisher messagePublisher) {
        this.messagePublisher = messagePublisher;
    }

    @Bean
    public Consumer<Message<Payload>> consumer(RabbitmqConsumerRecoverer recoverer) {
        return new RabbitmqConsumer<>(messagePublisher::publish, recoverer);
    }
}
