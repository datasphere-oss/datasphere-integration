package com.datasphere.kafka;

public class KafkaException extends Exception
{
    public KafkaException(final String message) {
        super(message);
    }
    
    public KafkaException(final Throwable cause) {
        super(cause);
    }
    
    public KafkaException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
