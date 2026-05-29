package com.example.rabbitchannelleakdemo.config.amqp.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;

import java.util.UUID;

import static org.springframework.messaging.support.MessageBuilder.fromMessage;

@Slf4j
public record RabbitmqProducer(StreamBridge streamBridge, RabbitTemplate rabbitTemplate) {

    public PublishResult publish(String exchange, String routingKey, Message<?> message) throws PublishException {

        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());

        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, message, correlationData);

        } catch (AmqpException e) {
            throw new PublishException("Error publishing message to exchange: " + exchange
                                       + " with routingKey: " + routingKey, e);
        }

        return new PublishResult(correlationData);
    }

    public PublishResult publish(String bindingName, Message<?> message) throws PublishException {

        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        Message<?> finalMessage = fromMessage(message)
            .setHeader(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, correlationData)
            .build();
        boolean sent = streamBridge.send(bindingName, finalMessage);

        if (!sent) {
            throw new PublishException("Message could not be sent to RabbitMQ broker");
        }

        return new PublishResult(correlationData);
    }

}
