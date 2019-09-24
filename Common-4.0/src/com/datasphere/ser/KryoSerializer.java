package com.datasphere.ser;

import org.apache.log4j.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

public final class KryoSerializer
{
    private static Logger logger;
    public static ClassLoader loader;
    private static final transient ThreadLocal<Kryo> threadSerializers;
    
    public static void setClassLoader(final ClassLoader aLoader) {
        KryoSerializer.loader = aLoader;
    }
    
    public static Kryo getSerializer() {
        return KryoSerializer.threadSerializers.get();
    }
    
    public static void register(final Class<?>... classes) {
        for (final Class<?> clazz : classes) {
            getSerializer().register((Class)clazz);
        }
    }
    
    public static byte[] write(final Object obj) {
        try {
            final Output out = new Output(8192, 1048576);
            getSerializer().writeClassAndObject(out, obj);
            return out.toBytes();
        }
        catch (Throwable t) {
            KryoSerializer.logger.error((Object)("Thread: " + Thread.currentThread().getName() + " Exception: " + t));
            throw t;
        }
    }
    
    public static Object read(final byte[] bytes) {
        try {
            final Input in = new Input(bytes);
            return getSerializer().readClassAndObject(in);
        }
        catch (Throwable t) {
            KryoSerializer.logger.error((Object)("Thread: " + Thread.currentThread().getName() + " Exception: " + t));
            KryoSerializer.logger.error((Object)t);
            throw t;
        }
    }
    
    public static Object read(final byte[] bytes, final int offset, final int len) {
        try {
            final byte[] b = new byte[len];
            System.arraycopy(bytes, offset, b, 0, len);
            final Input in = new Input(b);
            return getSerializer().readClassAndObject(in);
        }
        catch (Throwable t) {
            KryoSerializer.logger.error((Object)("Thread: " + Thread.currentThread().getName() + " Exception: " + t));
            KryoSerializer.logger.error((Object)t);
            throw t;
        }
    }
    
    static {
        KryoSerializer.logger = Logger.getLogger((Class)KryoSerializer.class);
        KryoSerializer.loader = null;
        threadSerializers = new ThreadLocal<Kryo>() {
            @Override
            protected Kryo initialValue() {
                final Kryo kryo = new Kryo();
                if (KryoSerializer.loader != null) {
                    kryo.setClassLoader(KryoSerializer.loader);
                }
                return kryo;
            }
        };
        getSerializer().setRegistrationRequired(false);
    }
}
