package com.datasphere.classloading;

import org.apache.log4j.*;
import com.datasphere.ser.*;
import java.util.*;
import javassist.*;
import java.io.*;

public class BundleLoader extends DistributedClassLoader
{
    static Logger logger;
    Map<String, Class<?>> loadedClasses;
    long ts;
    
    public BundleLoader(final ClassLoader parent, final boolean isClient) {
        super(parent, isClient);
        this.ts = System.currentTimeMillis();
        this.pool = new BundlePool(this, ((DistributedClassLoader)this.getParent()).pool);
        this.pool.childFirstLookup = true;
        this.loadedClasses = new HashMap<String, Class<?>>();
    }
    
    @Override
    protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
        if (BundleLoader.logger.isTraceEnabled()) {
            BundleLoader.logger.trace((Object)(this.getName() + ": load class " + name));
        }
        synchronized (this.loadedClasses) {
            if (this.loadedClasses.containsKey(name)) {
                final Class<?> clazz = this.loadedClasses.get(name);
                if (NoLongerValid.class.equals(clazz)) {
                    return this.getParent().loadClass(name);
                }
                return clazz;
            }
        }
        final ClassDefinition def = this.getClassDefinition(name);
        if (def != null && def.getBundleName().equals(this.getName())) {
            final Class<?> clazz = this.defineClass(name, this.getName(), def.getByteCode(), 0, def.getByteCode().length);
            if (def.getClassId() != -1) {
                KryoSingleton.get().addClassRegistration(clazz, def.getClassId());
            }
            return clazz;
        }
        return this.getParent().loadClass(name);
    }
    
    public Class<?> defineClass(final String name, final String loaderName, final byte[] b, final int off, final int len) throws ClassFormatError {
        Class<?> clazz = null;
        synchronized (this.loadedClasses) {
            if (this.loadedClasses.containsKey(name)) {
                if (NoLongerValid.class.equals(clazz)) {
                    clazz = ((WALoader)this.getParent()).defineClass(name, loaderName, b, off, len);
                }
                else {
                    clazz = this.loadedClasses.get(name);
                }
            }
            else {
                clazz = this.defineClass(name, b, off, len);
                this.loadedClasses.put(name, clazz);
            }
        }
        return clazz;
    }
    
    @Override
    public String toString() {
        return this.getName() + "-" + this.ts;
    }
    
    void removeClasses() {
        synchronized (this.loadedClasses) {
            for (final Map.Entry<String, Class<?>> entry : this.loadedClasses.entrySet()) {
                if (!entry.getValue().equals(NoLongerValid.class)) {
                    KryoSingleton.get().removeClassRegistration(entry.getValue());
                    this.loadedClasses.put(entry.getKey(), NoLongerValid.class);
                }
            }
        }
    }
    
    static {
        BundleLoader.logger = Logger.getLogger((Class)BundleLoader.class);
    }
    
    class BundlePool extends ClassPool
    {
        BundleLoader peerLoader;
        Map<String, CtClass> loadedCtClasses;
        
        public BundlePool(final BundleLoader bLoader, final ClassPool parent) {
            super(parent);
            this.peerLoader = bLoader;
            this.loadedCtClasses = new HashMap<String, CtClass>();
        }
        
        public CtClass get(final String name) throws NotFoundException {
            if (BundleLoader.logger.isTraceEnabled()) {
                BundleLoader.logger.trace((Object)(this.peerLoader.getName() + " get CtClass " + name));
            }
            synchronized (this.loadedCtClasses) {
                try {
                    final CtClass sClazz = super.get(name);
                    if (sClazz != null) {
                        if (BundleLoader.logger.isTraceEnabled()) {
                            BundleLoader.logger.trace((Object)(this.peerLoader.getName() + " found CtClass " + name + " in cache"));
                        }
                        return sClazz;
                    }
                }
                catch (NotFoundException nfe) {
                    if (BundleLoader.logger.isTraceEnabled()) {
                        BundleLoader.logger.trace((Object)(this.peerLoader.getName() + " CtClass " + name + " not in cache"));
                    }
                }
                if (this.loadedCtClasses.containsKey(name)) {
                    if (BundleLoader.logger.isTraceEnabled()) {
                        BundleLoader.logger.trace((Object)(this.peerLoader.getName() + " CtClass " + name + " in previously loaded classes"));
                    }
                    return this.loadedCtClasses.get(name);
                }
                final ClassDefinition def = this.peerLoader.getClassDefinition(name);
                if (def != null) {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(def.getByteCode());
                    CtClass ctClazz;
                    try {
                        ctClazz = this.makeClass((InputStream)bais);
                    }
                    catch (IOException | RuntimeException ex) {
                        throw new RuntimeException(this.peerLoader.getName() + ": Invalid bytecode for class " + name, ex);
                    }
                    this.loadedCtClasses.put(name, ctClazz);
                    if (BundleLoader.logger.isTraceEnabled()) {
                        BundleLoader.logger.trace((Object)(this.peerLoader.getName() + " CtClass " + name + " created from byte code"));
                    }
                    return ctClazz;
                }
            }
            if (BundleLoader.logger.isTraceEnabled()) {
                BundleLoader.logger.trace((Object)(this.peerLoader.getName() + " asking parent for CtClass " + name));
            }
            return this.parent.get(name);
        }
    }
    
    static class NoLongerValid
    {
    }
}
