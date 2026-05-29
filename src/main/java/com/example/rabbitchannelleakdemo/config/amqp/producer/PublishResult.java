package com.example.rabbitchannelleakdemo.config.amqp.producer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.amqp.rabbit.connection.CorrelationData;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Getter
@AllArgsConstructor
public class PublishResult {

    private CorrelationData correlationData;

    public void awaitConfirm(long timeoutMillis) throws PublishException {

        try {
            boolean ack = correlationData.getFuture()
                    .get(timeoutMillis, TimeUnit.MILLISECONDS)
                    .isAck();

            if (!ack) {
                throw new PublishException("Message was not acknowledged by RabbitMQ broker");
            }

        } catch (CancellationException | TimeoutException | ExecutionException | InterruptedException e) {
            throw new PublishException("Error waiting for message confirmation", e);
        }
    }

}
