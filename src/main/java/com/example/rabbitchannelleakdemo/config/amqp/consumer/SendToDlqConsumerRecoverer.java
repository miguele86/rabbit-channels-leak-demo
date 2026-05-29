package com.example.rabbitchannelleakdemo.config.amqp.consumer;

import com.example.rabbitchannelleakdemo.config.amqp.producer.PublishException;
import com.example.rabbitchannelleakdemo.config.amqp.producer.RabbitmqProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;

import static org.springframework.core.NestedExceptionUtils.getRootCause;

@Slf4j
public class SendToDlqConsumerRecoverer {

    private final RabbitmqProducer producer;

    private final Duration confirmTimeout = Duration.ofSeconds(10);

    public SendToDlqConsumerRecoverer(RabbitmqProducer rabbitMqExecutionMessageProducer) {
        this.producer = rabbitMqExecutionMessageProducer;
    }

    public boolean recoverMessage(Message<?> message, Throwable throwable) {

        var exchangeName = message.getHeaders().get(AmqpHeaders.RECEIVED_EXCHANGE, String.class);
        var queueName = message.getHeaders().get(AmqpHeaders.CONSUMER_QUEUE, String.class);
        var routingKey = message.getHeaders().get(AmqpHeaders.RECEIVED_ROUTING_KEY, String.class);

        var exceptionMessage = throwable.getMessage();
        var stackTrace = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stackTrace));

        try {
            var result = producer.publish("DLX", queueName, MessageBuilder.fromMessage(message)
                    .setHeader("x-original-exchange", exchangeName)
                    .setHeader("x-original-queue", queueName)
                    .setHeader("x-original-routing-key", routingKey)
                    .setHeader("x-exception-message", exceptionMessage)
                    .setHeader("x-exception-stacktrace", stackTrace.toString())
                    .build());

            log.warn("Send to 'DLX' using routingKey '{}' with root cause '{}'", queueName, getRootMessage(throwable),
                throwable);

            result.awaitConfirm(confirmTimeout.toMillis());

            log.info("SendToDlq recoverer success");
            return true;

        } catch (PublishException e) {
            log.warn("Send to DLQ was not acked. Will be managed via RabbitMq nack", e);
            return false;
        }
    }

    private static String getRootMessage(@NonNull Throwable throwable) {
        Throwable rootCause = getRootCause(throwable);
        return rootCause != null ? rootCause.getMessage() : throwable.getMessage();
    }
}
