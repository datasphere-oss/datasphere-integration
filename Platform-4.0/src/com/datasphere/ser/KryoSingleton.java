package com.datasphere.ser;

import org.apache.log4j.*;

import com.datasphere.runtime.channels.*;
import com.datasphere.runtime.containers.*;
import javax.persistence.criteria.*;
import com.datasphere.hd.*;
import com.datasphere.runtime.monitor.*;
import com.datasphere.runtime.*;
import org.apache.avro.generic.*;
import com.datasphere.proc.events.*;
import java.util.*;

import com.datasphere.cache.*;
import com.datasphere.classloading.*;
import com.datasphere.distribution.*;
import com.datasphere.event.*;
import com.datasphere.historicalcache.*;
import com.datasphere.jmqmessaging.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import org.objenesis.strategy.*;

import java.security.*;
import java.security.spec.*;
import java.io.*;

public class KryoSingleton
{
    public static final int PLATFORM_REGISTRATION_START_INDEX = 150;
    public static final int PLATFORM_REGISTRATION_MAX_SIZE = 200;
    private static Logger logger;
    private static ThreadLocal<KryoThreadLocal> threadKpes;
    private static KryoSingleton INSTANCE;
    private static ReloadableClassResolver resolver;
    private static KryoThreadLocal defaultKryo;
    static final int nrOfEvents = 1;
    static final String characters = "abcdefghijklmnopqrstuvwxyz";
    
    public static synchronized KryoSingleton get() {
        return KryoSingleton.INSTANCE;
    }
    
    private KryoSingleton() {
        if (KryoSingleton.INSTANCE != null) {
            throw new RuntimeException("Not a valid Initialization of Kryo");
        }
        KryoSingleton.threadKpes = new ThreadLocal<KryoThreadLocal>() {
            @Override
            protected KryoThreadLocal initialValue() {
                final KryoThreadLocal ktl = new KryoThreadLocal(KryoSingleton.resolver);
                ktl.initializeCiphers();
                return ktl;
            }
        };
        KryoSingleton.resolver = new ReloadableClassResolver();
        synchronized (KryoSingleton.defaultKryo = KryoSingleton.threadKpes.get()) {
            this.initKryo(KryoSingleton.defaultKryo.kryo);
        }
    }
    
    public void removeClassRegistration(final Class<?> clazz) {
        synchronized (KryoSingleton.defaultKryo) {
            ((ReloadableClassResolver)KryoSingleton.defaultKryo.kryo.getClassResolver()).removeClassRegistration(clazz);
        }
    }
    
    public void addClassRegistration(final Class<?> clazz, final int id) {
        synchronized (KryoSingleton.defaultKryo) {
            if (id != -1) {
                KryoSingleton.defaultKryo.kryo.register((Class)clazz, id);
            }
            else {
                KryoSingleton.defaultKryo.kryo.register((Class)clazz);
            }
        }
    }
    
    public void dumpClassIds() {
        synchronized (KryoSingleton.defaultKryo) {
            ((ReloadableClassResolver)KryoSingleton.defaultKryo.kryo.getClassResolver()).dumpClassIds();
        }
    }
    
    public boolean isClassRegistered(final Class<?> clazz) {
        synchronized (KryoSingleton.defaultKryo) {
            return KryoSingleton.defaultKryo.kryo.getClassResolver().getRegistration((Class)clazz) != null;
        }
    }
    
    public void initKryo(final Kryo kryo) {
        CommonObjectSpace.registerClasses(kryo);
        int index = 150;
        kryo.register((Class)Cache.class, index++);
        kryo.register((Class)HQueue.class, index++);
        kryo.register((Class)DistributedSubscriber.class, index++);
        kryo.register((Class)ZMQChannel.class, index++);
        kryo.register((Class)TaskEvent.class, index++);
        kryo.register((Class)StreamEvent.class, index++);
        kryo.register((Class)StreamTaskEvent.class, index++);
        kryo.register((Class)HQueueEvent.class, index++);
        kryo.register((Class)ShowStreamEvent.class, index++);
        kryo.register((Class)From.class, index++);
        kryo.register((Class)AlertEvent.class, index++);
        kryo.register((Class)ClusterTestEvent.class, index++);
        kryo.register((Class)JsonNodeEvent.class, index++);
        kryo.register((Class)Log4JEvent.class, index++);
        kryo.register((Class)ObjectArrayEvent.class);
        kryo.register((Class)StringArrayEvent.class, index++);
        kryo.register((Class)ClusterTestEvent.class, index++);
        kryo.register((Class)DynamicEvent.class, index++);
        kryo.register((Class)HD.class, index++);
        kryo.register((Class)HDKey.class, index++);
        kryo.register((Class)HDContext.class, index++);
        kryo.register((Class)EventJson.class, index++);
        kryo.register((Class)ExceptionEvent.class, index++);
        kryo.register((Class)MonitorEvent.class, index++);
        kryo.register((Class)MonitorBatchEvent.class, index++);
        kryo.register((Class)WindowsLogEvent.class, index++);
        kryo.register((Class)CollectdEvent.class, index++);
        kryo.register((Class)Executor.ExecutionRequest.class, index++);
        kryo.register((Class)Executor.ExecutionResponse.class, index++);
        kryo.register((Class)CacheAccessor.CacheEntry.class, index++);
        kryo.register((Class)CacheAccessor.CacheIterator.class, index++);
        kryo.register((Class)CacheAccessor.Clear.class, index++);
        kryo.register((Class)CacheAccessor.ContainsKey.class, index++);
        kryo.register((Class)CacheAccessor.Get.class, index++);
        kryo.register((Class)CacheAccessor.GetAll.class, index++);
        kryo.register((Class)CacheAccessor.GetAndPut.class, index++);
        kryo.register((Class)CacheAccessor.GetAndRemove.class, index++);
        kryo.register((Class)CacheAccessor.GetAndReplace.class, index++);
        kryo.register((Class)CacheAccessor.GetCacheInfo.class, index++);
        kryo.register((Class)CacheAccessor.ContainsKey.class, index++);
        kryo.register((Class)CacheAccessor.GetKeys.class, index++);
        kryo.register((Class)CacheAccessor.Invoke.class, index++);
        kryo.register((Class)CacheAccessor.Put.class, index++);
        kryo.register((Class)CacheAccessor.PutIfAbsent.class, index++);
        kryo.register((Class)CacheAccessor.Query.class, index++);
        kryo.register((Class)CacheAccessor.QueryStats.class, index++);
        kryo.register((Class)CacheAccessor.Remove.class, index++);
        kryo.register((Class)CacheAccessor.RemoveWithValue.class, index++);
        kryo.register((Class)CacheAccessor.Replace.class, index++);
        kryo.register((Class)CacheAccessor.SimpleQuery.class, index++);
        kryo.register((Class)CacheAccessor.Size.class, index++);
        kryo.register((Class)QueryManager.InsertCacheRecord.class, index++);
        kryo.register((Class)AvroEvent.class, index++);
        kryo.register((Class)LagMarker.class, index++);
        kryo.register((Class)GenericRecord.class, index++);
        kryo.register((Class)GenericData.Record.class, (Serializer)new AbstractAvroSerializer(), index++);
        kryo.register((Class)OPCUADataChangeEvent.class, index++);
    }
    
    public static Object read(byte[] bytes, final boolean isEncrypted) {
        final KryoThreadLocal entry = KryoSingleton.threadKpes.get();
        entry.isUsed = true;
        if (isEncrypted) {
            bytes = entry.getDecryptedBytes(bytes);
        }
        entry.input.setBuffer(bytes);
        final Object currentReadResult = entry.kryo.readClassAndObject(entry.input);
        entry.isUsed = false;
        return currentReadResult;
    }
    
    public static byte[] write(final Object obj, final boolean isEncrypted) {
        final KryoThreadLocal entry = KryoSingleton.threadKpes.get();
        entry.isUsed = true;
        byte[] currentBytes = null;
        entry.output.clear();
        entry.kryo.writeClassAndObject(entry.output, obj);
        currentBytes = entry.output.toBytes();
        if (isEncrypted) {
            currentBytes = entry.getEncryptedBytes(currentBytes);
        }
        entry.isUsed = false;
        return currentBytes;
    }
    
    public static String generateString(final int nrOfBytes) {
        final Random random = new Random();
        final char[] text = new char[nrOfBytes];
        for (int i = 0; i < nrOfBytes; ++i) {
            text[i] = "abcdefghijklmnopqrstuvwxyz".charAt(random.nextInt("abcdefghijklmnopqrstuvwxyz".length()));
        }
        return new String(text);
    }
    
    static {
        KryoSingleton.logger = Logger.getLogger((Class)KryoSingleton.class);
        KryoSingleton.INSTANCE = new KryoSingleton();
    }
    
    public static class ThreadSafeKryo extends Kryo
    {
        public Serializer getDefaultSerializer(final Class type) {
            final Serializer s = new ThreadSafeSerializer(type);
            return s;
        }
        
        public Serializer getSuperDefaultSerializer(final Class type) {
            return super.getDefaultSerializer(type);
        }
        
        public ThreadSafeKryo() {
        }
        
        public ThreadSafeKryo(final ClassResolver classResolver, final ReferenceResolver referenceResolver) {
            super(classResolver, referenceResolver);
        }
        
        public ThreadSafeKryo(final ReferenceResolver referenceResolver) {
            super(referenceResolver);
        }
    }
    
    public static class ThreadSafeSerializer extends Serializer
    {
        final Class type;
        private final ThreadLocal<Serializer> threadSerializers;
        
        public ThreadSafeSerializer(final Class type) {
            this.threadSerializers = new ThreadLocal<Serializer>();
            this.type = type;
        }
        
        private Serializer getThreadSerializer(final ThreadSafeKryo kryo) {
            Serializer s = this.threadSerializers.get();
            if (s == null) {
                s = kryo.getSuperDefaultSerializer(this.type);
                this.threadSerializers.set(s);
            }
            return s;
        }
        
        public void write(final Kryo kryo, final Output output, final Object object) {
            final Serializer s = this.getThreadSerializer((ThreadSafeKryo)kryo);
            s.write(kryo, output, object);
        }
        
        public Object read(final Kryo kryo, final Input input, final Class type) {
            final Serializer s = this.getThreadSerializer((ThreadSafeKryo)kryo);
            return s.read(kryo, input, type);
        }
    }
    
    public static class KryoThreadLocal
    {
        public ThreadSafeKryo kryo;
        public Input input;
        public Output output;
        public boolean isUsed;
        public Cipher cipherEncrypt;
        public Cipher cipherDecrypt;
        public Object encryptLock;
        public Object decryptLock;
        private static final byte[] AES_KEYS;
        private static final byte[] IV_BYTES;
        private static final SecretKeySpec keySpec;
        private static final IvParameterSpec ivSpec;
        private static final String ENCRY_STD = "AES/CBC/PKCS5Padding";
        private static Logger logger;
        
        public KryoThreadLocal(final ReloadableClassResolver resolver) {
            this.isUsed = false;
            this.cipherEncrypt = null;
            this.cipherDecrypt = null;
            this.encryptLock = new Object();
            this.decryptLock = new Object();
            (this.kryo = new ThreadSafeKryo((ClassResolver)resolver, null)).setReferences(false);
            this.kryo.setRegistrationRequired(false);
            this.kryo.setInstantiatorStrategy((InstantiatorStrategy)new StdInstantiatorStrategy());
            this.kryo.setClassLoader((ClassLoader)WALoader.get());
            this.output = new Output(8192, -1);
            this.input = new Input();
        }
        
        public void initializeCiphers() {
            try {
                (this.cipherEncrypt = Cipher.getInstance("AES/CBC/PKCS5Padding")).init(1, KryoThreadLocal.keySpec, KryoThreadLocal.ivSpec);
                (this.cipherDecrypt = Cipher.getInstance("AES/CBC/PKCS5Padding")).init(2, KryoThreadLocal.keySpec, KryoThreadLocal.ivSpec);
            }
            catch (Exception ex) {
                KryoThreadLocal.logger.error((Object)"Failed to create cipher for AES/CBC/PKCS5Padding", (Throwable)ex);
                throw new RuntimeException("Failed to create cipher for encryption.", ex);
            }
        }
        
        public byte[] getEncryptedBytes(final byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return bytes;
            }
            synchronized (this.encryptLock) {
                try {
                    if (KryoThreadLocal.logger.isTraceEnabled()) {
                        KryoThreadLocal.logger.trace((Object)("encrypted bytes : " + new String(this.cipherEncrypt.doFinal(bytes))));
                    }
                    return this.cipherEncrypt.doFinal(bytes);
                }
                catch (Exception ex) {
                    KryoThreadLocal.logger.error((Object)"Exception while encrypting data ", (Throwable)ex);
                    throw new RuntimeException("Exception while encrypting data ", ex);
                }
            }
        }
        
        public byte[] getDecryptedBytes(final byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return bytes;
            }
            synchronized (this.decryptLock) {
                try {
                    if (KryoThreadLocal.logger.isTraceEnabled()) {
                        KryoThreadLocal.logger.trace((Object)("decrypted bytes : " + new String(this.cipherDecrypt.doFinal(bytes))));
                    }
                    return this.cipherDecrypt.doFinal(bytes);
                }
                catch (Exception ex) {
                    KryoThreadLocal.logger.error((Object)"Exception while decrypting data ", (Throwable)ex);
                    throw new RuntimeException("Exception while decrypting data ", ex);
                }
            }
        }
        
        @Override
        public String toString() {
            try {
                return "input.size:" + this.input.available() + " , output:" + this.output.total() + ", kryo:" + (this.kryo != null);
            }
            catch (IOException ex) {
                return "";
            }
        }
        
        static {
            AES_KEYS = "GunturuMirapakai".getBytes();
            IV_BYTES = "MarathaSrikhanda".getBytes();
            keySpec = new SecretKeySpec(KryoThreadLocal.AES_KEYS, "AES");
            ivSpec = new IvParameterSpec(KryoThreadLocal.IV_BYTES);
            KryoThreadLocal.logger = Logger.getLogger((Class)KryoThreadLocal.class);
        }
    }
}
