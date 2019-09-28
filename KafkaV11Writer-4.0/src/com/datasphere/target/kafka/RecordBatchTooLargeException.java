package com.datasphere.target.kafka;


public class RecordBatchTooLargeException extends Exception
{
    public RecordBatchTooLargeException(final String msg) {
        super(msg);
    }
}

