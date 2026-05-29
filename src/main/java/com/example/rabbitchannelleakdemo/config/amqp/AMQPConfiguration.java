package com.example.rabbitchannelleakdemo.config.amqp;

import com.example.rabbitchannelleakdemo.config.amqp.consumer.SendToDlqConsumerRecoverer;
import com.example.rabbitchannelleakdemo.config.amqp.producer.RabbitmqProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.SimpleAmqpHeaderMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;

@Slf4j
@Configuration
public class AMQPConfiguration {

    @Bean
    public MessagingMessageConverter amqpMessageConverter(ObjectMapper objectMapper) {
        return new MessagingMessageConverter(new Jackson2JsonMessageConverter(objectMapper), new SimpleAmqpHeaderMapper());
    }

    @Bean
    public BeanPostProcessor connectionFactoryCustomizer(
        @Value("${spring.application.name}") String applicationName
    ) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(@Nullable Object bean, @Nullable String beanName) {
                if (bean instanceof CachingConnectionFactory cachingConnectionFactory) {
                    cachingConnectionFactory.setConnectionNameStrategy(connectionFactory -> applicationName);
                }
                return bean;
            }
        };
    }

    @Bean
    public RabbitmqProducer rabbitmqProducer(
        CachingConnectionFactory connectionFactory,
        MessagingMessageConverter amqpMessageConverter,
        ApplicationContext context,
        StreamBridge streamBridge
    ) {
        final var rabbitTemplate = buildRabbitTemplate(connectionFactory, amqpMessageConverter, context);

        return new RabbitmqProducer(streamBridge, rabbitTemplate);
    }

    private RabbitTemplate buildRabbitTemplate(
        CachingConnectionFactory connectionFactory,
        MessagingMessageConverter amqpMessageConverter,
        ApplicationContext context
    ) {

        final var rabbitTemplate = new RabbitTemplate(connectionFactory.getPublisherConnectionFactory());

        rabbitTemplate.setMessageConverter(amqpMessageConverter);
        rabbitTemplate.setUsePublisherConnection(true);
        rabbitTemplate.setApplicationContext(context);
        rabbitTemplate.afterPropertiesSet();

        return rabbitTemplate;
    }

    @Bean
    public SendToDlqConsumerRecoverer sendToDlqRecoverer(RabbitmqProducer rabbitmqProducer) {
        return new SendToDlqConsumerRecoverer(rabbitmqProducer);
    }

}
