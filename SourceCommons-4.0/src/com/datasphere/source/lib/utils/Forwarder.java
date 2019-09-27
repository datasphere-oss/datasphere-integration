package com.datasphere.source.lib.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.lib.socket.SocketReader;
import com.datasphere.source.lib.type.EventStatus;

public class Forwarder extends SocketReader
{
    Property prop;
    Reader reader;
    Object data;
    ByteBuffer eventBuffer;
    byte[] serializedEventArray;
    static Logger logger;
    static boolean closeConnection;
    
    public Forwarder(final Property prop) throws AdapterException {
        super(null, prop);
        this.prop = prop;
        (this.reader = Reader.FileReader(prop)).position(null, true);
    }
    
    public void connectToSubscriber() throws AdapterException {
        this.connect();
        if (Forwarder.logger.isInfoEnabled()) {
            Forwarder.logger.info((Object)("Connection to subscriber is successful at remote address " + this.prop.remoteaddress + " and port no " + this.prop.portno));
        }
    }
    
    public EventStatus getEventStatus() throws AdapterException {
        try {
            final Object block = this.reader.readBlock();
            this.data = block;
            if (block != null) {
                this.eventBuffer = (ByteBuffer)this.data;
                return EventStatus.PUBLISH_EVENT;
            }
        }
        catch (AdapterException e) {
            this.close();
            throw e;
        }
        return EventStatus.NO_EVENT;
    }
    
    public void publishEvents() throws AdapterException {
        try {
            this.send(this.eventBuffer);
        }
        catch (AdapterException e) {
            this.close();
            throw e;
        }
        this.data = null;
        this.eventBuffer.clear();
    }
    
    public void closeSubscriberConnection() throws AdapterException {
        try {
            this.close();
            this.reader.close();
        }
        catch (IOException e) {
            final AdapterException sourceException = new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
            throw sourceException;
        }
        if (Forwarder.logger.isInfoEnabled()) {
            Forwarder.logger.info((Object)("Connection with subscriber has been closed successfully at remote address " + this.prop.remoteaddress + " and port no " + this.prop.portno));
        }
    }
    
    public void receiveEvents() {
    }
    
    public static void main(final String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Forwarder.closeConnection = true;
                if (Forwarder.logger.isInfoEnabled()) {
                    Forwarder.logger.info((Object)"Forwarder has been shutdown successfully");
                }
            }
        });
        final Properties configProperties = new Properties();
        String propertyFile;
        if (args.length > 0) {
            propertyFile = args[0];
        }
        else {
            propertyFile = "../conf/forwarder.properties";
        }
        InputStream propertyStream = null;
        try {
            propertyStream = new FileInputStream(new File(propertyFile));
        }
        catch (FileNotFoundException e) {
            final AdapterException se = new AdapterException(Error.INVALID_DIRECTORY, (Throwable)e);
            Forwarder.logger.error((Object)(se.getErrorMessage() + ". Unable to locate configuration file needed by the forwarder."));
            System.exit(0);
        }
        try {
            configProperties.load(propertyStream);
        }
        catch (IOException e2) {
            final AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e2);
            Forwarder.logger.error((Object)(se.getErrorMessage() + ". Problem loading configuration file."));
            System.exit(0);
        }
        final Map<String, Object> map = new HashMap<String, Object>();
        for (final String name : configProperties.stringPropertyNames()) {
            map.put(name, configProperties.getProperty(name));
        }
        final Property prop = new Property(map);
        boolean reInitializeForwarder = false;
        do {
            try {
                final Forwarder lf = new Forwarder(prop);
                lf.connectToSubscriber();
                do {
                    if (lf.getEventStatus() != EventStatus.NO_EVENT) {
                        lf.publishEvents();
                    }
                    else {
                        try {
                            Thread.sleep(prop.eofdelay);
                        }
                        catch (InterruptedException e3) {
                            final AdapterException se2 = new AdapterException(Error.GENERIC_INTERRUPT_EXCEPTION, (Throwable)e3);
                            throw se2;
                        }
                    }
                } while (!Forwarder.closeConnection);
                lf.closeSubscriberConnection();
            }
            catch (AdapterException e4) {
                Forwarder.logger.error((Object)e4.getErrorMessage());
                if (e4.getType() == Error.HOST_CONNECTION_DROPPED) {
                    if (Forwarder.logger.isInfoEnabled()) {
                        Forwarder.logger.info((Object)"Reinitializing Forwarder. Attempting to connect back to host...");
                    }
                    reInitializeForwarder = true;
                }
            }
        } while (reInitializeForwarder);
    }
    
    static {
        Forwarder.logger = Logger.getLogger((Class)Forwarder.class);
        Forwarder.closeConnection = false;
    }
}
