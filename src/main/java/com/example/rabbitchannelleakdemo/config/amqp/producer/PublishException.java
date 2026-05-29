package com.example.rabbitchannelleakdemo.config.amqp.producer;

public class PublishException extends Exception {

    public PublishException(String message) {
        super(message);
    }

    public PublishException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
