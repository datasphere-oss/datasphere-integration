package com.datasphere.distribution;

import org.apache.log4j.*;
import com.esotericsoftware.kryo.io.*;
import java.io.*;
import com.esotericsoftware.kryo.util.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryonet.*;
import java.nio.*;

public class ReloadableKryoSerialization implements Serialization
{
    static Logger logger;
    private Kryo kryo;
    private ReloadableClassResolver resolver;
    private Object lock;
    private Input input;
    private Output output;
    private final ByteBufferInputStream byteBufferInputStream;
    private final ByteBufferOutputStream byteBufferOutputStream;
    
    public ReloadableKryoSerialization() {
        this.lock = new Object();
        this.byteBufferInputStream = new ByteBufferInputStream();
        this.byteBufferOutputStream = new ByteBufferOutputStream();
        this.reloadKryo();
        this.input = new Input((InputStream)this.byteBufferInputStream, 512);
        this.output = new Output((OutputStream)this.byteBufferOutputStream, 512);
    }
    
    public void removeClassRegistration(final Class<?> clazz) {
        this.resolver.removeClassRegistration(clazz);
    }
    
    public void addClassRegistration(final Class<?> clazz, final int id) {
        this.kryo.register((Class)clazz, id);
    }
    
    public void reloadKryo() {
        synchronized (this.lock) {
            if (this.kryo == null) {
                this.resolver = new ReloadableClassResolver();
                this.kryo = new Kryo((ClassResolver)this.resolver, (ReferenceResolver)new MapReferenceResolver());
            }
            else {
                this.kryo.reset();
            }
            this.kryo.register((Class)FrameworkMessage.RegisterTCP.class);
            this.kryo.register((Class)FrameworkMessage.RegisterUDP.class);
            this.kryo.register((Class)FrameworkMessage.KeepAlive.class);
            this.kryo.register((Class)FrameworkMessage.DiscoverHost.class);
            this.kryo.register((Class)FrameworkMessage.DiscoverReply.class);
            this.kryo.register((Class)FrameworkMessage.Ping.class);
        }
    }
    
    public Kryo getKryo() {
        synchronized (this.lock) {
            return this.kryo;
        }
    }
    
    public void write(final Connection connection, final ByteBuffer buffer, final Object object) {
        synchronized (this.lock) {
            try {
                synchronized (this.output) {
                    this.byteBufferOutputStream.setByteBuffer(buffer);
                    this.kryo.getContext().put((Object)"connection", (Object)connection);
                    this.kryo.writeClassAndObject(this.output, object);
                    this.output.flush();
                }
            }
            catch (Throwable t) {
                ReloadableKryoSerialization.logger.error((Object)"Problem Writing Object", t);
            }
        }
    }
    
    public Object read(final Connection connection, final ByteBuffer buffer) {
        synchronized (this.lock) {
            try {
                synchronized (this.input) {
                    this.byteBufferInputStream.setByteBuffer(buffer);
                    this.kryo.getContext().put((Object)"connection", (Object)connection);
                    return this.kryo.readClassAndObject(this.input);
                }
            }
            catch (Throwable t) {
                ReloadableKryoSerialization.logger.error((Object)"Problem Reading Object", t);
                return null;
            }
        }
    }
    
    public void writeLength(final ByteBuffer buffer, final int length) {
        buffer.putInt(length);
    }
    
    public int readLength(final ByteBuffer buffer) {
        return buffer.getInt();
    }
    
    public int getLengthLength() {
        return 4;
    }
    
    static {
        ReloadableKryoSerialization.logger = Logger.getLogger((Class)ReloadableKryoSerialization.class);
    }
}
