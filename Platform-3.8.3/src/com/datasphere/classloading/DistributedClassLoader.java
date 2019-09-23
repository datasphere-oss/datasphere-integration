package com.datasphere.classloading;

import org.apache.log4j.*;
import javassist.*;
import com.hazelcast.core.*;
import com.datasphere.metaRepository.*;

public abstract class DistributedClassLoader extends ClassLoader
{
    static Logger logger;
    final boolean isClient;
    ClassPool pool;
    String name;
    
    public DistributedClassLoader() {
        this(DistributedClassLoader.class.getClassLoader(), true, false);
    }
    
    public DistributedClassLoader(final boolean isClient) {
        this(DistributedClassLoader.class.getClassLoader(), true, isClient);
    }
    
    public DistributedClassLoader(final ClassLoader parent) {
        this(parent, true, false);
    }
    
    public DistributedClassLoader(final ClassLoader parent, final boolean isClient) {
        this(parent, true, isClient);
    }
    
    private DistributedClassLoader(final ClassLoader parent, final boolean isBridgeLoader, final boolean isClient) {
        super(parent);
        this.isClient = isClient;
    }
    
    @Override
    public Class<?> loadClass(final String name) throws ClassNotFoundException {
        if (DistributedClassLoader.logger.isTraceEnabled()) {
            DistributedClassLoader.logger.trace((Object)(this.getName() + ": load class " + name));
        }
        return this.loadClass(name, false);
    }
    
    public ClassPool getPool() {
        return this.pool;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public BundleDefinition getBundleDefinition(final String name) {
        return (BundleDefinition)getBundles().get((Object)name);
    }
    
    public ClassDefinition getClassDefinition(final String name) {
        return (ClassDefinition)getClasses().get((Object)name);
    }
    
    public String getBundleNameForClass(final String name) {
        return (String)getClassToBundleResolver().get((Object)name);
    }
    
    protected static IMap<String, BundleDefinition> getBundles() {
        final IMap<String, BundleDefinition> bundles = HazelcastSingleton.get().getMap("#bundles");
        return bundles;
    }
    
    public static IMap<String, ClassDefinition> getClasses() {
        final IMap<String, ClassDefinition> classes = HazelcastSingleton.get().getMap("#classes");
        return classes;
    }
    
    protected static IMap<String, String> getClassToBundleResolver() {
        final IMap<String, String> classToBundleResolver = HazelcastSingleton.get().getMap("#classResolver");
        return classToBundleResolver;
    }
    
    static {
        DistributedClassLoader.logger = Logger.getLogger((Class)DistributedClassLoader.class);
    }
}
