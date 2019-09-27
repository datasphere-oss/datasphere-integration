package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.io.common.JMSCommon;
import com.datasphere.source.lib.prop.Property;

public class JMSReader extends QueueReader implements MessageListener
{
    private JMSCommon jmsCommon;
    private Logger logger;
    
    public JMSReader(final Property prop) throws AdapterException {
        this.logger = Logger.getLogger((Class)JMSReader.class);
        this.property = prop;
        this.jmsCommon = new JMSCommon(this.property);
    }
    
    private void initializeTopic() throws AdapterException {
        try {
            final Topic topic = this.jmsCommon.retrieveTopicInstance();
            final TopicSubscriber subscriber = ((TopicSession)this.jmsCommon.getSession()).createSubscriber(topic);
            subscriber.setMessageListener((MessageListener)this);
            this.jmsCommon.startConnection();
            this.name(this.property.topic);
            if (this.logger.isTraceEnabled()) {
                this.logger.trace((Object)("JMS topic " + this.property.topic + " has been initialized"));
            }
        }
        catch (JMSException e) {
            throw new AdapterException(Error.GENERIC_EXCEPTION, (Throwable)e);
        }
    }
    
    private void initializeQueue() throws AdapterException {
        try {
            final Queue msgQueue = this.jmsCommon.retrieveQueueInstance();
            final QueueReceiver receiver = ((QueueSession)this.jmsCommon.getSession()).createReceiver(msgQueue);
            receiver.setMessageListener((MessageListener)this);
            this.jmsCommon.startConnection();
            this.name(this.property.queueName);
            if (this.logger.isTraceEnabled()) {
                this.logger.trace((Object)("JMS queue " + this.property.queueName + " has been intiialized"));
            }
        }
        catch (JMSException e) {
            throw new AdapterException(Error.GENERIC_EXCEPTION, (Throwable)e);
        }
    }
    
    @Override
    protected void init() throws AdapterException {
        this.blockSize(this.property.blocksize * 1024);
        super.init();
        this.jmsCommon.initialize();
        if (this.property.topic != null && !this.property.topic.isEmpty()) {
            this.initializeTopic();
        }
        else {
            if (this.property.queueName == null || this.property.queueName.trim().isEmpty()) {
                throw new AdapterException("Please provide a topic or queue name to read messages");
            }
            this.initializeQueue();
        }
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("JMSReader is initialized with following properties\nUserName -  [" + this.property.userName + "]\nContext - [" + this.property.context + "]\nProvider - [" + this.property.provider + "]"));
        }
    }
    
    @Override
    public void close() throws IOException {
        try {
            this.jmsCommon.closeConnection();
        }
        catch (AdapterException e) {
            throw new IOException((Throwable)e);
        }
    }
    
    public void onMessage(final Message msg) {
        try {
            if (msg instanceof TextMessage) {
                final String txt = ((TextMessage)msg).getText();
                if (this.logger.isTraceEnabled()) {
                    this.logger.trace((Object)("JMS message of type TextMessage containing " + txt.getBytes().length + " has been received"));
                }
                final ByteBuffer buffer = ByteBuffer.allocate(txt.getBytes().length);
                buffer.put(txt.getBytes());
                buffer.flip();
                super.enqueue(buffer);
            }
            else {
                this.logger.warn((Object)("Received message of type \"" + msg.getJMSType() + "\" is not supported by JMSReader, only TextMessage is supported"));
            }
        }
        catch (JMSException exp) {
            exp.printStackTrace();
        }
    }
    
    private void enqueue(final ByteBuffer buffer) {
        int sizeToCopy = 0;
        do {
            final ByteBuffer tmpBuffer = ByteBuffer.allocate(this.blockSize());
            sizeToCopy = tmpBuffer.limit();
            if (buffer.limit() - buffer.position() < sizeToCopy) {
                sizeToCopy = buffer.limit() - buffer.position();
            }
            buffer.get(tmpBuffer.array(), 0, sizeToCopy);
            tmpBuffer.limit(sizeToCopy);
            super.enqueue(tmpBuffer);
        } while (buffer.position() < buffer.limit());
    }
}
