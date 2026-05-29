package com.example.rabbitchannelleakdemo.config.amqp.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.CompositeRetryPolicy;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.springframework.amqp.support.AmqpHeaders.CHANNEL;
import static org.springframework.amqp.support.AmqpHeaders.DELIVERY_TAG;

@Slf4j
@RequiredArgsConstructor
public final class RabbitmqConsumer<T> implements Consumer<Message<T>> {

    @Setter
    private ExecutorService executorService;
    @Setter
    private RetryTemplate retryTemplate;

    private final Consumer<Message<T>> delegate;

    private final SendToDlqConsumerRecoverer recoverer;

    @PostConstruct
    void configure() {
        if (this.executorService == null) {
            this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        }

        if (this.retryTemplate == null) {
            // first time + 2 retries
            CompositeRetryPolicy retryPolicy = new CompositeRetryPolicy();
            retryPolicy.setPolicies(new RetryPolicy[]{
                new MaxAttemptsRetryPolicy(3),
            });

            // retry with intervals 300, 900 ms
            ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
            backOffPolicy.setMultiplier(3);
            backOffPolicy.setInitialInterval(300);

            RetryTemplate newRetryTemplate = new RetryTemplate();
            newRetryTemplate.setRetryPolicy(retryPolicy);
            newRetryTemplate.setBackOffPolicy(backOffPolicy);

            this.retryTemplate = newRetryTemplate;
        }
    }

    @PreDestroy
    void shutdown() throws InterruptedException {
        executorService.shutdown();
        boolean completed = executorService.awaitTermination(10, TimeUnit.SECONDS);

        if (completed) {
            log.info("RabbitMqExecutionMessageConsumer shutdown completed");

        } else {
            log.warn("RabbitMqExecutionMessageConsumer shutdown timed out. Uncompleted tasks will be redelivered by RabbitMQ");
        }
    }

    @Override
    public void accept(Message<T> message) {
        var channel = message.getHeaders().get(CHANNEL, Channel.class);
        var deliveryTag = message.getHeaders().get(DELIVERY_TAG, Long.class);
        assert channel != null && deliveryTag != null : "Consumer acknowledgement must be set to manual";

        try {

            executorService.execute(() -> {

                try {

                    boolean ok = retryTemplate.execute(retryContext -> {

                        if (retryContext.getRetryCount() > 0) {
                            log.warn("Retry [{}] from RabbitmqConsumer after delegate processing exception",
                                retryContext.getRetryCount(), retryContext.getLastThrowable());
                        }

                        log.info("Processing message with deliveryTag {}", deliveryTag);

                        messageProcessor(message);
                        return true;

                    }, retryContext -> recover(message, retryContext.getLastThrowable()));

                    if (ok) {
                        channel.basicAck(deliveryTag, false);

                    } else {
                        // Send to DLQ by RabbitMQ
                        channel.basicNack(deliveryTag, false, false);
                    }

                } catch (ShutdownSignalException | IOException exception) {
                    log.warn("Channel is closed, cannot ack message with deliveryTag {}, " +
                             "will be retried by RabbitMQ, may generate a duplicate message", deliveryTag, exception);

                } catch (Exception unexpected) {
                    log.error("Unexpected error processing message with deliveryTag {}", deliveryTag, unexpected);
                }
            });

        } catch (RejectedExecutionException rejectedExecutionException) {
            log.debug("Message received after shutdown, will be redelivered, deliveryTag {}", deliveryTag, rejectedExecutionException);

            try {
                channel.basicNack(deliveryTag, false, true);

            } catch (Exception exception) {
                // channel is closed, should be redelivered by rabbitmq.
                log.debug("Failed to nack to RabbitMq, deliveryTag {}", deliveryTag, exception);
            }
        }
    }

    private void messageProcessor(Message<T> message) {
        delegate.accept(message);
    }

    private boolean recover(Message<?> message, Throwable throwable) {
        try {
            return recoverer.recoverMessage(message, throwable);

        } catch (Exception exception) {
            log.error("Error recovering message, it will be NACKed without requeue to be sent to DLQ by RabbitMQ");
            return false;
        }
    }

}
