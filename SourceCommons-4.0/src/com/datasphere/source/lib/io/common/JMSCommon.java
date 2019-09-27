package com.datasphere.source.lib.io.common;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.log4j.Logger;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.prop.Property;

public class JMSCommon
{
    private Property property;
    private InitialContext namingContext;
    private String connectionFactoryName;
    private final String TOPIC_CONN_FACTORY = "TopicConnectionFactory";
    private final String QUEUE_CONN_FACTORY = "QueueConnectionFactory";
    private Connection connection;
    private Session session;
    private Logger logger;
    private boolean authenticateBroker;
    
    public JMSCommon(final Property property) {
        this.logger = Logger.getLogger((Class)JMSCommon.class);
        this.authenticateBroker = false;
        this.property = property;
        if (property.userName != null) {
            if (property.password != null) {
                this.authenticateBroker = true;
            }
            else {
                this.logger.warn((Object)("Password isn't specified for the user " + property.userName + ". Connecting to JMS provider/broker with default user identity"));
            }
        }
        else if (property.password != null) {
            this.logger.warn((Object)"System cannot detect username from the specified password. Connecting to JMS provider/broker with default user identity");
        }
    }
    
    public void initialize() throws AdapterException {
        final Properties environmentParameters = new Properties();
        environmentParameters.put("java.naming.factory.initial", this.property.context);
        environmentParameters.put("java.naming.provider.url", this.property.provider);
        try {
            this.namingContext = new InitialContext(environmentParameters);
            this.connectionFactoryName = this.property.connectionFactoryName;
        }
        catch (NamingException e) {
            throw new AdapterException("Problem in constructing JMS initial context for the ContextFactory " + this.property.context, (Throwable)e);
        }
        if (this.logger.isInfoEnabled()) {
            this.logger.info((Object)("JMS initial context has been intialized with the specified naming context " + this.property.context));
        }
    }
    
    public Queue retrieveQueueInstance() throws AdapterException {
        if (this.connectionFactoryName == null) {
            this.connectionFactoryName = "QueueConnectionFactory";
        }
        Queue queue = null;
        QueueConnection queueConnection = null;
        try {
            final QueueConnectionFactory queueConnectionFactory = (QueueConnectionFactory)this.namingContext.lookup(this.connectionFactoryName);
            if (this.authenticateBroker) {
                queueConnection = queueConnectionFactory.createQueueConnection(this.property.userName, this.property.password);
            }
            else {
                queueConnection = queueConnectionFactory.createQueueConnection();
            }
            this.connection = (Connection)queueConnection;
            final QueueSession queueSession = queueConnection.createQueueSession(false, 1);
            this.session = (Session)queueSession;
            queue = (Queue)this.namingContext.lookup(this.property.queueName);
            if (this.logger.isInfoEnabled()) {
                this.logger.info((Object)("Queue instance " + queue.getQueueName() + " is retrieved for creating a consumer or producer"));
            }
        }
        catch (NamingException | JMSException e) {
            throw new AdapterException("Problem in retrieveing queue instance for the queue name " + this.property.queueName, (Throwable)e);
        }
        return queue;
    }
    
    public void startConnection() throws AdapterException {
        try {
            this.connection.start();
        }
        catch (JMSException e) {
            throw new AdapterException("Problem in starting this connection's delivery of incoming messages", (Throwable)e);
        }
    }
    
    public void closeConnection() throws AdapterException {
        try {
            if (this.connection != null) {
                this.connection.close();
            }
            if (this.logger.isInfoEnabled()) {
                this.logger.info((Object)("Connection " + this.connection.toString() + " to JMS provider is closed"));
            }
        }
        catch (JMSException e) {
            throw new AdapterException("Probem in closing this connection", (Throwable)e);
        }
    }
    
    public Topic retrieveTopicInstance() throws AdapterException {
        if (this.connectionFactoryName == null) {
            this.connectionFactoryName = "TopicConnectionFactory";
        }
        Topic topic = null;
        TopicConnection topicConnection = null;
        try {
            final TopicConnectionFactory connectionFactory = (TopicConnectionFactory)this.namingContext.lookup(this.connectionFactoryName);
            if (this.authenticateBroker) {
                topicConnection = connectionFactory.createTopicConnection(this.property.userName, this.property.password);
            }
            else {
                topicConnection = connectionFactory.createTopicConnection();
            }
            this.connection = (Connection)topicConnection;
            final TopicSession topicSession = topicConnection.createTopicSession(false, 1);
            this.session = (Session)topicSession;
            topic = (Topic)this.namingContext.lookup(this.property.topic);
            if (this.logger.isTraceEnabled()) {
                this.logger.trace((Object)("Topic instance " + topic.getTopicName() + " is retrieved for creating a consumer or producer"));
            }
        }
        catch (NamingException | JMSException e) {
            throw new AdapterException("Problem in retrieving topic instance for the topic name " + this.property.topic, (Throwable)e);
        }
        return topic;
    }
    
    public Session getSession() {
        return this.session;
    }
}
