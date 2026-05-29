package com.example.rabbitchannelleakdemo.service;

import com.example.rabbitchannelleakdemo.config.amqp.producer.PublishException;
import com.example.rabbitchannelleakdemo.config.amqp.producer.RabbitmqProducer;
import com.example.rabbitchannelleakdemo.model.Payload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
@Retryable(maxAttempts = 3, backoff = @Backoff(delay = 200))
public class MessagePublisher {

    private final RabbitmqProducer producer;

    public void publish(Message<Payload> message) {
        try {
            log.info("Publishing message");
            producer.publish("producer-out-0", message).awaitConfirm(100);
        } catch (PublishException e) {
            log.error("Error publishing message");
            throw new RuntimeException(e);
        }
    }

    @Recover
    public void recover(Exception exception, Message<Payload> message) {
        log.error("Unable to publish the message");
        throw new RuntimeException(exception);
    }
}
