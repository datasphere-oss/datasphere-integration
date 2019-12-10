package com.datasphere.classloading;

import org.apache.log4j.*;
import java.lang.reflect.*;
import com.datasphere.metaRepository.*;
import java.util.jar.*;
import java.util.zip.*;
import com.hazelcast.transaction.*;
import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

import java.io.*;

import com.datasphere.event.*;
import com.esotericsoftware.kryo.*;
import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.meta.*;
import javassist.*;
import com.datasphere.hd.*;
import com.hazelcast.core.*;

public class HDLoader extends DistributedClassLoader
{
    public static final String ALL_IDS = "#allIds";
    public static final String CLASS_IDS = "#classIds";
    static Logger logger;
    private static volatile HDLoader instance;
    private final Map<String, BundleLoader> bundleLoaders;
    private Map<String, Class<?>> primitiveLookup;
    final Object primitiveLookupLock;
    
    public static HDLoader get(final ClassLoader parent, final boolean isClient) {
        if (HDLoader.instance == null) {
            synchronized (HDLoader.class) {
                if (HDLoader.instance == null) {
                    HDLoader.instance = new HDLoader((parent == null) ? DistributedClassLoader.class.getClassLoader() : parent, isClient);
                }
            }
        }
        return HDLoader.instance;
    }
    
    public static void shutdown() {
        HDLoader.instance = null;
    }
    
    public static HDLoader get(final ClassLoader parent) {
        return get(parent, false);
    }
    
    public static HDLoader get(final boolean isClient) {
        return get(DistributedClassLoader.class.getClassLoader(), isClient);
    }
    
    public static HDLoader get() {
        return get(null, false);
    }
    
    public static HDLoaderDelegate getDelegate() {
        return new HDLoaderDelegate();
    }
    
    private HDLoader(final ClassLoader parent, final boolean isClient) {
        super(parent, isClient);
        this.bundleLoaders = new HashMap<String, BundleLoader>();
        this.primitiveLookup = null;
        this.primitiveLookupLock = new Object();
        this.pool = new BridgePool(this);
        this.pool.childFirstLookup = true;
        this.setName("HDLoader");
    }
    
    public void init() {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        bundles.addEntryListener((EntryListener)new BundleMonitor(), true);
    }
    
    public void lockClass(final String name) {
        final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
        classes.lock(name);
    }
    
    public void unlockClass(final String name) {
        final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
        classes.unlock(name);
    }
    
    public void lockBundle(final String name) {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        bundles.lock(name);
    }
    
    public void unlockBundle(final String name) {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        bundles.unlock(name);
    }
    
    void initializePrimitiveLookup() {
        synchronized (this.primitiveLookupLock) {
            if (this.primitiveLookup == null) {
                (this.primitiveLookup = new HashMap<String, Class<?>>()).put(Boolean.TYPE.getName(), Boolean.TYPE);
                this.primitiveLookup.put(Byte.TYPE.getName(), Byte.TYPE);
                this.primitiveLookup.put(Character.TYPE.getName(), Character.TYPE);
                this.primitiveLookup.put(Double.TYPE.getName(), Double.TYPE);
                this.primitiveLookup.put(Float.TYPE.getName(), Float.TYPE);
                this.primitiveLookup.put(Integer.TYPE.getName(), Integer.TYPE);
                this.primitiveLookup.put(Long.TYPE.getName(), Long.TYPE);
                this.primitiveLookup.put(Short.TYPE.getName(), Short.TYPE);
                this.primitiveLookup.put("Z", Boolean.TYPE);
                this.primitiveLookup.put("B", Byte.TYPE);
                this.primitiveLookup.put("C", Character.TYPE);
                this.primitiveLookup.put("D", Double.TYPE);
                this.primitiveLookup.put("F", Float.TYPE);
                this.primitiveLookup.put("I", Integer.TYPE);
                this.primitiveLookup.put("J", Long.TYPE);
                this.primitiveLookup.put("S", Short.TYPE);
            }
        }
    }
    
    Class<?> checkForPrimitive(final String name) {
        if (this.primitiveLookup == null) {
            this.initializePrimitiveLookup();
        }
        return this.primitiveLookup.get(name);
    }
    
    @Override
    protected Class<?> loadClass(String name, final boolean resolve) throws ClassNotFoundException {
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace((this.getName() + ": load class " + name));
        }
        boolean isArray = false;
        int arrayDimensions = 0;
        if (name.startsWith("[")) {
            while (name.startsWith("[")) {
                name = name.substring(1);
                ++arrayDimensions;
            }
            isArray = true;
        }
        else if (name.endsWith("[]")) {
            while (name.endsWith("[]")) {
                name = name.substring(0, name.length() - 2);
                ++arrayDimensions;
            }
            isArray = true;
        }
        Class<?> clazz = this.checkForPrimitive(name);
        if (clazz == null) {
            if (name.startsWith("L") && name.endsWith(";")) {
                name = name.substring(1, name.length() - 1);
            }
            try {
                clazz = this.getParent().loadClass(name);
            }
            catch (ClassNotFoundException ex) {}
            if (clazz == null) {
                final BundleLoader bl = this.getBundleLoaderForClass(name);
                if (bl == null) {
                    throw new ClassNotFoundException("Could not load class " + name);
                }
                clazz = bl.loadClass(name);
            }
        }
        if (isArray) {
            while (arrayDimensions-- > 0) {
                clazz = Array.newInstance(clazz, 0).getClass();
            }
        }
        return clazz;
    }
    
    public final Class<?> defineClass(final String name, final String loaderName, final byte[] b, final int off, final int len) throws ClassFormatError {
        final BundleLoader bl = this.getBundleLoaderForClass(name);
        if (bl != null) {
            return bl.defineClass(name, loaderName, b, off, len);
        }
        throw new RuntimeException(this.getName() + ": Could not define class " + name);
    }
    
    @Override
    public InputStream getResourceAsStream(final String name) {
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace((this.getName() + ": get resource " + name));
        }
        String className = name.replaceAll("[/]", ".");
        if (className.endsWith(".class")) {
            className = className.substring(0, className.lastIndexOf(46));
        }
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace((this.getName() + ": get resource checking for class " + className));
        }
        final byte[] bytecode = this.getClassBytes(className);
        if (bytecode != null) {
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace((this.getName() + ": get resource returning bytes for " + name));
            }
            final ByteArrayInputStream bais = new ByteArrayInputStream(bytecode);
            return bais;
        }
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace((this.getName() + ": get resource delegating to parent for for " + name));
        }
        return this.getParent().getResourceAsStream(name);
    }
    
    public boolean isManagedClass(final String name) {
        final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
        return classes.containsKey(name);
    }
    
    public boolean isSystemClass(final String name) {
        final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
        return !classes.containsKey(name);
    }
    
    public boolean isGeneratedClass(final String name) {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        final IMap<String, String> classToBundleResolver = DistributedClassLoader.getClassToBundleResolver();
        if (this.isManagedClass(name)) {
            final String bundleName = (String)classToBundleResolver.get(name);
            if (bundleName != null) {
                final BundleDefinition def = (BundleDefinition)bundles.get(bundleName);
                if (def != null) {
                    return def.getType() != BundleDefinition.Type.jar;
                }
            }
        }
        return false;
    }
    
    public boolean isExistingClass(final String name) {
        try {
            final Class<?> clazz = this.loadClass(name);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("Loaded class " + clazz + " from name " + name + " using loader " + clazz.getClassLoader()));
            }
            return true;
        }
        catch (ClassNotFoundException e) {
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("Could not load class " + name + " using loader " + this));
            }
            return false;
        }
    }
    
    public byte[] getClassBytes(final String name) {
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace((this.getName() + ": get class bytes " + name));
        }
        final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
        final ClassDefinition def = (ClassDefinition)classes.get(name);
        if (def != null) {
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace((this.getName() + ": get class bytes returning bytes for " + name));
            }
            return def.getByteCode();
        }
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace((this.getName() + ": get class bytes not found for " + name));
        }
        return null;
    }
    
    private byte[] inputStreamToByteArray(final InputStream is) throws Exception {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final byte[] data = new byte[16384];
        int nRead;
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        return buffer.toByteArray();
    }
    
    private String getClassName(final String path, final byte[] bytecode) {
        try {
            final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytecode));
            dis.readLong();
            final int cpcnt = (dis.readShort() & 0xFFFF) - 1;
            final int[] classArray = new int[cpcnt];
            final String[] strings = new String[cpcnt];
            for (int i = 0; i < cpcnt; ++i) {
                final int t = dis.read();
                if (t == 7) {
                    classArray[i] = (dis.readShort() & 0xFFFF);
                }
                else if (t == 1) {
                    strings[i] = dis.readUTF();
                }
                else if (t == 5 || t == 6) {
                    dis.readLong();
                    ++i;
                }
                else if (t == 8) {
                    dis.readShort();
                }
                else {
                    dis.readInt();
                }
            }
            dis.readShort();
            return strings[classArray[(dis.readShort() & 0xFFFF) - 1] - 1].replace('/', '.');
        }
        catch (Exception e) {
            if (HDLoader.logger.isDebugEnabled()) {
                HDLoader.logger.debug(("Problem reading classname from: " + path));
            }
            return null;
        }
    }
    
    public int getClassId(final String className) {
        final IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
        idMap.lock("#allIds");
        Integer id = (Integer)idMap.get(className);
        if (id == null) {
            id = (Integer)idMap.get("#allIds");
            if (id == null) {
                final ILock lock = HazelcastSingleton.get().getLock("classIdsLock");
                lock.lock();
                try {
                    id = (Integer)idMap.get("#allIds");
                    if (id == null) {
                        id = 350;
                        idMap.put("#allIds", id);
                    }
                }
                finally {
                    lock.unlock();
                }
            }
            ++id;
            idMap.put("#allIds", id);
            idMap.put(className, id);
        }
        idMap.unlock("#allIds");
        return id;
    }
    
    public void setClassId(final String className, final int classId) {
        final IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
        idMap.lock("#allIds");
        idMap.put(className, classId);
        Integer curMaxId = (Integer)idMap.get("#allIds");
        if (curMaxId == null) {
            curMaxId = 350;
        }
        if (curMaxId <= classId) {
            idMap.put("#allIds", classId);
        }
        idMap.unlock("#allIds");
    }
    
    public void setMaxClassId(final Integer maxId) {
        final IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
        idMap.lock("#allIds");
        final Integer currentMaxId = (Integer)idMap.get("#allIds");
        if (currentMaxId == null) {
            idMap.put("#allIds", maxId);
        }
        else {
            HDLoader.logger.warn(("Unexpected warn statement while setting max class id, current max id: " + currentMaxId + ", max id: " + maxId));
        }
        idMap.unlock("#allIds");
    }
    
    public void addJar(final String appName, final String path, final String name) throws Exception {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
        final IMap<String, String> classToBundleResolver = DistributedClassLoader.getClassToBundleResolver();
        String uri = this.getBundleUri(appName, BundleDefinition.Type.jar, name);
        BundleDefinition def = (BundleDefinition)bundles.get(uri);
        if (def != null) {
            throw new IllegalArgumentException("Can't add " + uri + " it already exists");
        }
        def = new BundleDefinition(BundleDefinition.Type.jar, appName, path, name);
        uri = def.getUri();
        try (final JarFile jar = new JarFile(path)) {
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                if (entry.isDirectory()) {
                    continue;
                }
                final InputStream is = jar.getInputStream(entry);
                final byte[] bytecode = this.inputStreamToByteArray(is);
                final String className = this.getClassName(entry.getName(), bytecode);
                if (className == null) {
                    continue;
                }
                ClassDefinition classDef = this.getClassDefinition(className);
                if (classDef != null) {
                    throw new Exception("Class " + className + " is already defined for bundle " + classDef.getBundleName());
                }
                classDef = new ClassDefinition(uri, className, -1, bytecode);
                classes.put(className, classDef);
                classToBundleResolver.put(className, uri);
                def.addClassName(className);
            }
        }
        bundles.put(uri, def);
    }
    
    public String createIfNotExistsBundleDefinition(final String appName, final BundleDefinition.Type type, final String name) {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        final String uri = BundleDefinition.getUri(appName, type, name);
        BundleDefinition def = (BundleDefinition)bundles.get(uri);
        if (def == null) {
            def = new BundleDefinition(type, appName, name);
            bundles.put(uri, def);
        }
        return def.getUri();
    }
    
    public String getBundleUri(final String appName, final BundleDefinition.Type type, final String name) {
        return BundleDefinition.getUri(appName, type, name);
    }
    
    public boolean isExistingBundle(final String uri) {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        return bundles.get(uri) != null;
    }
    
    public String addBundleDefinition(final String appName, final BundleDefinition.Type type, final String name) {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        final String uri = this.getBundleUri(appName, type, name);
        BundleDefinition def = (BundleDefinition)bundles.get(uri);
        if (def != null) {
            throw new IllegalArgumentException("Can't add " + uri + " it already exists");
        }
        def = new BundleDefinition(type, appName, name);
        bundles.put(uri, def);
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("Bundle created: " + uri));
        }
        return def.getUri();
    }
    
    public void addBundleClass(final String bundleUri, final String className, final byte[] bytecode, final boolean register) {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
        final IMap<String, String> classToBundleResolver = DistributedClassLoader.getClassToBundleResolver();
        final BundleDefinition def = (BundleDefinition)bundles.get(bundleUri);
        if (def == null) {
            throw new IllegalArgumentException("Can't add class " + className + " to non existant " + bundleUri);
        }
        ClassDefinition classDef = this.getClassDefinition(className);
        if (classDef != null) {
            throw new IllegalArgumentException("Can't add class " + className + " to " + bundleUri + " it already exists in " + classDef.getBundleName());
        }
        classDef = new ClassDefinition(bundleUri, className, register ? this.getClassId(className) : -1, bytecode);
        classes.put(className, classDef);
        classToBundleResolver.put(className, bundleUri);
        def.addClassName(className);
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("Bundle " + bundleUri + " class added: " + className));
        }
        bundles.put(bundleUri, def);
    }
    
    private BundleLoader getBundleLoader(final String uri) {
        BundleLoader loader;
        synchronized (this.bundleLoaders) {
            final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
            final BundleDefinition def = (BundleDefinition)bundles.get(uri);
            if (def == null) {
                throw new IllegalArgumentException("Can't get loader for " + uri + " it does not exists");
            }
            loader = this.bundleLoaders.get(uri);
            if (loader == null) {
                loader = new BundleLoader(this, this.isClient);
                loader.setName(uri);
                this.bundleLoaders.put(uri, loader);
            }
        }
        return loader;
    }
    
    public ClassPool getBundlePool(final String uri) {
        final BundleLoader loader = this.getBundleLoader(uri);
        return loader.getPool();
    }
    
    public BundleLoader getBundleLoaderForClass(final String name) {
        final IMap<String, String> classToBundleResolver = DistributedClassLoader.getClassToBundleResolver();
        if (classToBundleResolver.containsKey(name)) {
            final String uri = (String)classToBundleResolver.get(name);
            return this.getBundleLoader(uri);
        }
        return null;
    }
    
    public void removeJar(final String appName, final String name) throws Exception {
        final String uri = this.getBundleUri(appName, BundleDefinition.Type.jar, name);
        this.removeBundle(uri);
    }
    
    public void removeBundle(final String uri) {
        synchronized (this.bundleLoaders) {
            final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
            final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
            final IMap<String, String> classToBundleResolver = DistributedClassLoader.getClassToBundleResolver();
            final BundleDefinition def = (BundleDefinition)bundles.get(uri);
            final TransactionOptions options = new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
            final TransactionContext context = HazelcastSingleton.get().newTransactionContext(options);
            context.beginTransaction();
            try {
                if (def != null) {
                    bundles.remove(uri);
                    for (final String className : def.getClassNames()) {
                        classes.remove(className);
                        classToBundleResolver.remove(className);
                        if (HDLoader.logger.isTraceEnabled()) {
                            HDLoader.logger.trace(("Removed class " + className));
                        }
                    }
                }
                context.commitTransaction();
            }
            catch (Throwable t) {
                context.rollbackTransaction();
            }
        }
    }
    
    public void removeBundle(final String appName, final BundleDefinition.Type type, final String name) {
        final String uri = this.getBundleUri(appName, type, name);
        this.removeBundle(uri);
    }
    
    private String getObject(final CtClass type, final String value) {
        if (CtClass.booleanType.equals(type)) {
            return " new Boolean(" + value + ")";
        }
        if (CtClass.byteType.equals(type)) {
            return " new Byte(" + value + ")";
        }
        if (CtClass.charType.equals(type)) {
            return " new Character(" + value + ")";
        }
        if (CtClass.shortType.equals(type)) {
            return " new Short(" + value + ")";
        }
        if (CtClass.intType.equals(type)) {
            return " new Integer(" + value + ")";
        }
        if (CtClass.longType.equals(type)) {
            return " new Long(" + value + ")";
        }
        if (CtClass.floatType.equals(type)) {
            return " new Float(" + value + ")";
        }
        if (CtClass.doubleType.equals(type)) {
            return " new Double(" + value + ")";
        }
        return value;
    }
    
    private String getCast(final CtClass type, final String value) {
        if (CtClass.booleanType.equals(type)) {
            return "((Boolean) " + value + ").booleanValue()";
        }
        if (CtClass.byteType.equals(type)) {
            return "((Byte) " + value + ").byteValue()";
        }
        if (CtClass.charType.equals(type)) {
            return "((Character) " + value + ").charValue()";
        }
        if (CtClass.shortType.equals(type)) {
            return "((Short) " + value + ").shortValue()";
        }
        if (CtClass.intType.equals(type)) {
            return "((Integer) " + value + ").intValue()";
        }
        if (CtClass.longType.equals(type)) {
            return "((Long) " + value + ").longValue()";
        }
        if (CtClass.floatType.equals(type)) {
            return "((Float) " + value + ").floatValue()";
        }
        if (CtClass.doubleType.equals(type)) {
            return "((Double) " + value + ").doubleValue()";
        }
        if ("org.joda.time.DateTime".equals(type.getName())) {
            return "(" + value + " instanceof org.joda.time.DateTime ? (org.joda.time.DateTime) " + value + " : new org.joda.time.DateTime(" + value + "))";
        }
        return "(" + type.getName() + ") " + value;
    }
    
    private String getMethodName(final String type) {
        if ("java.lang.String".equals(type)) {
            return "val.toString()";
        }
        if ("char".equals(type)) {
            return "((Character)val).charValue()";
        }
        if ("short".equals(type)) {
            return "((Short)val).shortValue()";
        }
        if ("int".equals(type)) {
            return "((Integer)val).intValue()";
        }
        if ("double".equals(type)) {
            return "((Double)val).doubleValue()";
        }
        if ("float".equals(type)) {
            return "((Float)val).floatValue()";
        }
        if ("long".equals(type)) {
            return "((Long)val).longValue()";
        }
        if ("byte".equals(type)) {
            return "((Byte)val).byteValue()";
        }
        if ("boolean".equals(type)) {
            return "((Boolean)val).booleanValue()";
        }
        return "(" + type + ") val";
    }
    
    private String getMethodName1(final String type, final String fieldName) {
        if ("java.lang.String".equals(type)) {
            return "(" + type + ")ctx.get(\"" + fieldName + "\")";
        }
        if ("char".equals(type)) {
            return "(" + type + ")ctx.get(\"" + fieldName + "\")";
        }
        if ("short".equals(type)) {
            return "((Short)val).shortValue()";
        }
        if ("int".equals(type)) {
            return "((Integer)val).intValue()";
        }
        if ("double".equals(type)) {
            return "((Double)val).doubleValue()";
        }
        if ("float".equals(type)) {
            return "((Float)val).floatValue()";
        }
        if ("long".equals(type)) {
            return "((Long)val).longValue()";
        }
        if ("byte".equals(type)) {
            return "((Byte)val).byteValue()";
        }
        if ("boolean".equals(type)) {
            return "((Boolean)val).booleanValue()";
        }
        return "(" + type + ") val";
    }
    
    private static String getDataType(final String type) {
        if ("java.lang.String".equals(type)) {
            return "String";
        }
        if ("java.lang.Boolean".equals(type)) {
            return "Boolean";
        }
        return type.substring(0, 1).toUpperCase() + type.substring(1);
    }
    
    public void removeTypeClass(final String appName, final String className) {
        final String uri = this.getBundleUri(appName, BundleDefinition.Type.type, className);
        this.removeBundle(uri);
    }
    
    protected String getKryoWrite(final String indent, final CtClass type, final String value) throws Exception {
        if (CtClass.booleanType.equals(type)) {
            return indent + "output.writeBoolean(" + value + ");\n";
        }
        if (CtClass.byteType.equals(type)) {
            return indent + "output.writeByte(" + value + ");\n";
        }
        if (CtClass.charType.equals(type)) {
            return indent + "output.writeChar(" + value + ");\n";
        }
        if (CtClass.shortType.equals(type)) {
            return indent + "output.writeShort(" + value + ");\n";
        }
        if (CtClass.intType.equals(type)) {
            return indent + "output.writeInt(" + value + ");\n";
        }
        if (CtClass.longType.equals(type)) {
            return indent + "output.writeLong(" + value + ");\n";
        }
        if (CtClass.floatType.equals(type)) {
            return indent + "output.writeFloat(" + value + ");\n";
        }
        if (CtClass.doubleType.equals(type)) {
            return indent + "output.writeDouble(" + value + ");\n";
        }
        if ("java.lang.String".equals(type.getName())) {
            return indent + "output.writeString(" + value + ");\n";
        }
        return indent + "kryo.writeClassAndObject(output, " + value + ");\n";
    }
    
    protected String getKryoRead(final String indent, final CtClass type, final String value) throws Exception {
        if (CtClass.booleanType.equals(type)) {
            return indent + value + " = input.readBoolean();\n";
        }
        if (CtClass.byteType.equals(type)) {
            return indent + value + " = input.readByte();\n";
        }
        if (CtClass.charType.equals(type)) {
            return indent + value + " = input.readChar();\n";
        }
        if (CtClass.shortType.equals(type)) {
            return indent + value + " = input.readShort();\n";
        }
        if (CtClass.intType.equals(type)) {
            return indent + value + " = input.readInt();\n";
        }
        if (CtClass.longType.equals(type)) {
            return indent + value + " = input.readLong();\n";
        }
        if (CtClass.floatType.equals(type)) {
            return indent + value + " = input.readFloat();\n";
        }
        if (CtClass.doubleType.equals(type)) {
            return indent + value + " = input.readDouble();\n";
        }
        if ("java.lang.String".equals(type.getName())) {
            return indent + value + " = input.readString();\n";
        }
        return indent + value + " = (" + type.getName() + ") kryo.readClassAndObject(input);\n";
    }
    
    public BundleDefinition getTypeBundleDef(final String appName, final String className) {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        final String bundleUri = this.getBundleUri(appName, BundleDefinition.Type.type, className);
        return (BundleDefinition)bundles.get(bundleUri);
    }
    
    private String makeSetFromHDMethod(final Map<String, String> fields, final ClassPool pool) throws NotFoundException {
        final StringBuilder sb = new StringBuilder();
        sb.append("public void convertFromHDToEvent(long timestamp,").append(UUID.class.getCanonicalName()).append(" id, String key,java.util.Map context) {\n");
        sb.append("setTimeStamp(timestamp); \n");
        sb.append("setID(id); \n");
        sb.append("setKey(key); \n");
        for (final Map.Entry<String, String> field : fields.entrySet()) {
            final String name = field.getKey();
            final String type = field.getValue();
            final CtClass cl = pool.getCtClass(type);
            sb.append("this.").append(name).append(" = ").append(this.getCast(cl, "context.get(\"" + name + "\")")).append(";\n");
        }
        sb.append("}\n");
        return sb.toString();
    }
    
    public void addTypeClass(final String appName, final String className, final Map<String, String> fields) throws Exception {
        try {
            final BundleDefinition def = this.getTypeBundleDef(appName, className);
            if (def != null) {
                throw new IllegalArgumentException("Class for type " + className + " is already defined");
            }
            final String bundleUri = this.addBundleDefinition(appName, BundleDefinition.Type.type, className);
            final BundleLoader bl = this.getBundleLoader(bundleUri);
            final ClassPool pool = bl.getPool();
            final CtClass bclass = pool.makeClass(className);
            bclass.addConstructor(CtNewConstructor.defaultConstructor(bclass));
            final CtClass sclass = pool.get(SimpleEvent.class.getCanonicalName());
            final CtClass hdConvertibleInterface = pool.get(HDConvertible.class.getCanonicalName());
            final CtClass[] interfaceList = { hdConvertibleInterface };
            bclass.setSuperclass(sclass);
            bclass.setInterfaces(interfaceList);
            final String eventConstructorSource = "public " + bclass.getSimpleName() + "(long timestamp) {\n  super(timestamp);\n  fieldIsSet = new byte[" + (fields.size() / 7 + 1) + "];\n}\n";
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("Event constructor is " + eventConstructorSource));
            }
            final CtConstructor eventConstructor = CtNewConstructor.make(eventConstructorSource, bclass);
            bclass.addConstructor(eventConstructor);
            final CtClass[] fclasses = new CtClass[fields.size()];
            final CtClass serClass = pool.get(Serializable.class.getName());
            bclass.addInterface(serClass);
            final CtClass kserClass = pool.get(KryoSerializable.class.getName());
            bclass.addInterface(kserClass);
            String getPayloadMethod = "public Object[] getPayload() {\n  return new Object[] {\n";
            String setPayloadMethod = "public void setPayload(Object[] payload) {\n";
            String kserWriteMethod = "public void write(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Output output) {\n  super.write(kryo, output);\n";
            String kserReadMethod = "public void read(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Input input) {\n  super.read(kryo, input);\n";
            String setFromContextMapMethod = "public boolean setFromContextMap(java.util.Map map) {\n";
            setFromContextMapMethod += "  timeStamp = ((java.lang.Long)map.get(\"timestamp\")).longValue();\n";
            setFromContextMapMethod += "  _wa_SimpleEvent_ID =  new com.datasphere.uuid.UUID((java.lang.String)map.get(\"uuid\"));\n";
            setFromContextMapMethod += "  key = (map.get(\"key\") == null ? null : map.get(\"key\").toString());\n";
            int i = 0;
            for (final Map.Entry<String, String> field : fields.entrySet()) {
                final String name = field.getKey();
                final String type = field.getValue();
                fclasses[i] = pool.getCtClass(type);
                final CtField bfield = new CtField(fclasses[i], name, bclass);
                bfield.setModifiers(1);
                bclass.addField(bfield);
                bclass.addMethod(CtNewMethod.getter("get" + name.substring(0, 1).toUpperCase() + name.substring(1), bfield));
                bclass.addMethod(CtNewMethod.setter("set" + name.substring(0, 1).toUpperCase() + name.substring(1), bfield));
                getPayloadMethod = getPayloadMethod + "  " + this.getObject(fclasses[i], name) + ((i == fields.size() - 1) ? "" : ",") + "\n";
                setPayloadMethod = setPayloadMethod + "  " + name + " = " + this.getCast(fclasses[i], "payload[" + i + "]") + ";\n";
                kserWriteMethod += this.getKryoWrite("  ", fclasses[i], name);
                kserReadMethod += this.getKryoRead("  ", fclasses[i], name);
                setFromContextMapMethod = setFromContextMapMethod + "  " + name + " = " + this.getCast(fclasses[i], "map.get(\"context-" + name + "\")") + ";\n";
                ++i;
            }
            getPayloadMethod += "  };\n}\n";
            setPayloadMethod += "}\n";
            kserWriteMethod += "}\n";
            kserReadMethod += "}\n";
            setFromContextMapMethod += "  return true;\n}\n";
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("getPayloadMethod: " + getPayloadMethod));
            }
            final CtMethod m = CtMethod.make(getPayloadMethod, bclass);
            bclass.addMethod(m);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(setPayloadMethod);
            }
            final CtMethod m2 = CtMethod.make(setPayloadMethod, bclass);
            bclass.addMethod(m2);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(kserWriteMethod);
            }
            final CtMethod m3 = CtMethod.make(kserWriteMethod, bclass);
            bclass.addMethod(m3);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(kserReadMethod);
            }
            final CtMethod m4 = CtMethod.make(kserReadMethod, bclass);
            bclass.addMethod(m4);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(setFromContextMapMethod);
            }
            final CtMethod m5 = CtMethod.make(setFromContextMapMethod, bclass);
            bclass.addMethod(m5);
            final String setFromHDMethod = this.makeSetFromHDMethod(fields, pool);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(setFromHDMethod);
            }
            final CtMethod m6 = CtMethod.make(setFromHDMethod, bclass);
            bclass.addMethod(m6);
            final CtField mapper = CtField.make("public static com.fasterxml.jackson.databind.ObjectMapper  mapper = com.datasphere.event.ObjectMapperFactory.newInstance();", bclass);
            bclass.addField(mapper);
            final String fromJSONsource = "public Object fromJSON(String json) {  return mapper.readValue(json, this.getClass());}";
            final CtMethod fromJSON = CtMethod.make(fromJSONsource, bclass);
            bclass.addMethod(fromJSON);
            final String toJSONsource = "public String toJSON() {  return mapper.writeValueAsString(this);}";
            final CtMethod toJSON = CtMethod.make(toJSONsource, bclass);
            bclass.addMethod(toJSON);
            final String toStringsource = "public String toString() {  return toJSON();}";
            final CtMethod toString = CtMethod.make(toStringsource, bclass);
            bclass.addMethod(toString);
            if (HDLoader.logger.isDebugEnabled()) {
                HDLoader.logger.debug(("bean class generated. name = " + bclass.getName() + ", toString = " + bclass));
            }
            final byte[] bytecode = bclass.toBytecode();
            final String report = CompilerUtils.verifyBytecode(bytecode, this);
            if (report != null) {
                throw new Error("Internal error: invalid bytecode\n" + report);
            }
            this.addBundleClass(bundleUri, className, bytecode, true);
            if (HDLoader.logger.isDebugEnabled()) {
                HDLoader.logger.debug(("Added to bundle className :" + className));
            }
        }
        catch (Exception e) {
            if (HDLoader.logger.isDebugEnabled()) {
                HDLoader.logger.debug(("Problem creating class for type " + className), (Throwable)e);
            }
            throw new Exception("Could not instantiate class for type " + className + ": " + e, e);
        }
    }
    
    public void addHDClass(final String className, final MetaInfo.Type metaInfo) throws Exception {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        if (metaInfo == null) {
            throw new ClassNotFoundException("metaInfo is null");
        }
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("trying to create a run time HD sub class : " + metaInfo.className));
        }
        final String appName = metaInfo.nsName;
        String bundleUri = this.getBundleUri(appName, BundleDefinition.Type.hd, className);
        final BundleDefinition def = (BundleDefinition)bundles.get(bundleUri);
        if (def != null) {
            throw new IllegalArgumentException("Class for type " + className + " is already defined");
        }
        bundleUri = this.addBundleDefinition(appName, BundleDefinition.Type.hd, className);
        final BundleLoader bl = this.getBundleLoader(bundleUri);
        final ClassPool pool = bl.getPool();
        final CtClass bclass = pool.makeClass(className);
        final CtClass sclass = pool.get(HD.class.getName());
        SerialVersionUID.setSerialVersionUID(bclass);
        bclass.setSuperclass(sclass);
        final int noOfFields = metaInfo.fields.size();
        final CtClass[] fclasses = new CtClass[noOfFields];
        String setContextSrc = "public void setContext(java.util.Map ctx) {\n";
        if (HDLoader.logger.isTraceEnabled()) {
            setContextSrc += "\tSystem.out.println(\"setcontext of runtime is called.\" + ctx.toString() );\n";
        }
        setContextSrc += "\tObject val = null;\n";
        final Map<String, String> fields = metaInfo.fields;
        int i = 0;
        for (final Map.Entry<String, String> field : fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            fclasses[i] = pool.getCtClass(fieldType);
            final CtField bfield = new CtField(fclasses[i], fieldName, bclass);
            bfield.setModifiers(1);
            bclass.addField(bfield);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("field name : " + fieldName + ", field.type : " + fieldType));
            }
            bclass.addMethod(CtNewMethod.getter("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), bfield));
            String setterMethodSrc = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + fieldType + " " + fieldName + ") {\n";
            setterMethodSrc = setterMethodSrc + "\tthis." + fieldName + " = " + fieldName + ";\n";
            setterMethodSrc = setterMethodSrc + "\tObject val = getWrappedValue(" + fieldName + ");\n";
            setterMethodSrc = setterMethodSrc + "\tcontext.map.put(\"" + fieldName + "\", val);\n";
            if (HDLoader.logger.isTraceEnabled()) {
                setterMethodSrc = setterMethodSrc + "\tSystem.out.println(\"added field '" + fieldName + "' with value = \" + val  );\n";
            }
            setterMethodSrc += "}\n";
            if (HDLoader.logger.isDebugEnabled()) {
                HDLoader.logger.debug(("setterMethodSrc for field : " + fieldName + " \n" + setterMethodSrc));
            }
            final CtMethod m = CtMethod.make(setterMethodSrc, bclass);
            bclass.addMethod(m);
            setContextSrc = setContextSrc + "\tval  = ctx.get(\"" + fieldName + "\");\n";
            setContextSrc += "\tif(val != null) {\n";
            setContextSrc = setContextSrc + "\t\tset" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + this.getMethodName(fieldType) + ");\n";
            setContextSrc += "\t}\n";
            setContextSrc += "\tval = null;\n";
            if (fieldType.equalsIgnoreCase("org.joda.time.DateTime")) {
                String jodaHibGetMth = "public long get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "AsLong( ) {\n";
                jodaHibGetMth = jodaHibGetMth + "\tif(" + fieldName + " == null) return 0L;\n";
                jodaHibGetMth = jodaHibGetMth + "\treturn this." + fieldName + ".getMillis();\n";
                jodaHibGetMth += "}";
                final CtMethod jmget = CtMethod.make(jodaHibGetMth, bclass);
                bclass.addMethod(jmget);
                String jodaHibSetMth = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "AsLong(long val) {\n";
                jodaHibSetMth = jodaHibSetMth + "\tthis.set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(new org.joda.time.DateTime(val));\n";
                jodaHibSetMth += "}";
                final CtMethod jmset = CtMethod.make(jodaHibSetMth, bclass);
                bclass.addMethod(jmset);
            }
            ++i;
        }
        setContextSrc += "}";
        final CtMethod setCntxMeth = CtMethod.make(setContextSrc, bclass);
        bclass.addMethod(setCntxMeth);
        if (HDLoader.logger.isDebugEnabled()) {
            HDLoader.logger.debug(("bean class generated. name = " + bclass.getName() + ", toString = " + bclass));
        }
        final byte[] bytecode = bclass.toBytecode();
        final String report = CompilerUtils.verifyBytecode(bytecode, this);
        if (report != null) {
            throw new Error("Internal error: invalid bytecode\n" + report);
        }
        this.addBundleClass(bundleUri, className, bytecode, true);
    }
    
    public void addHDContextClassNoEventType(final MetaInfo.Type metaInfo) throws Exception {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        if (metaInfo == null) {
            throw new ClassNotFoundException("metaInfo is null");
        }
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("inside instantiateHDContextClass : " + metaInfo.className));
        }
        final String className = metaInfo.className + "_hdContext";
        final String appName = metaInfo.nsName;
        String bundleUri = this.getBundleUri(appName, BundleDefinition.Type.context, className);
        final BundleDefinition def = (BundleDefinition)bundles.get(bundleUri);
        if (def != null) {
            throw new IllegalArgumentException("Class for type " + className + " is already defined");
        }
        bundleUri = this.addBundleDefinition(appName, BundleDefinition.Type.context, className);
        final BundleLoader bl = this.getBundleLoader(bundleUri);
        final ClassPool pool = bl.getPool();
        final CtClass bclass = pool.makeClass(className);
        bclass.addConstructor(CtNewConstructor.defaultConstructor(bclass));
        final CtClass sclass = pool.get(HDContext.class.getName());
        SerialVersionUID.setSerialVersionUID(bclass);
        bclass.setSuperclass(sclass);
        final int noOfFields = metaInfo.fields.size();
        final CtClass[] fclasses = new CtClass[noOfFields];
        final Map<String, String> fields = metaInfo.fields;
        String putMethodSrc = "public Object put(String key, Object val) {\n";
        int i = 0;
        for (final Map.Entry<String, String> field : fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            fclasses[i] = pool.getCtClass(fieldType);
            final CtField bfield = new CtField(fclasses[i], fieldName, bclass);
            bfield.setModifiers(1);
            bclass.addField(bfield);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("field name : " + fieldName + ", field.type : " + fieldType));
            }
            bclass.addMethod(CtNewMethod.getter("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), bfield));
            String setterMethodSrc = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + fieldType + " " + fieldName + ") {\n";
            setterMethodSrc = setterMethodSrc + "\tthis." + fieldName + " = " + fieldName + ";\n";
            setterMethodSrc = setterMethodSrc + "\tObject val = getWrappedValue(" + fieldName + ");\n";
            setterMethodSrc = setterMethodSrc + "\tmap.put(\"" + fieldName + "\", val);\n";
            setterMethodSrc += "}\n";
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("setterMethodSrc for field : " + fieldName + " \n" + setterMethodSrc));
            }
            final CtMethod m = CtMethod.make(setterMethodSrc, bclass);
            bclass.addMethod(m);
            putMethodSrc = putMethodSrc + "\tif(key.equals(\"" + fieldName + "\")){\n";
            putMethodSrc = putMethodSrc + "\t\tset" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + this.getMethodName(fieldType) + ");\n";
            putMethodSrc = putMethodSrc + "\t\treturn map.put(\"" + fieldName + "\",val);\n";
            putMethodSrc += "\t}\n";
            ++i;
        }
        final String fieldName2 = "jsonData";
        final String fieldType2 = "java.lang.String";
        final CtClass jsondataClass = pool.getCtClass(fieldType2);
        final CtField jsondataField = new CtField(jsondataClass, fieldName2, bclass);
        jsondataField.setModifiers(1);
        bclass.addField(jsondataField);
        bclass.addMethod(CtNewMethod.getter("get" + fieldName2.substring(0, 1).toUpperCase() + fieldName2.substring(1), jsondataField));
        String setterMethodSrc2 = "public void set" + fieldName2.substring(0, 1).toUpperCase() + fieldName2.substring(1) + "(" + fieldType2 + " " + fieldName2 + ") {\n";
        setterMethodSrc2 = setterMethodSrc2 + "\tthis." + fieldName2 + " = " + fieldName2 + ";\n";
        setterMethodSrc2 = setterMethodSrc2 + "\tObject val = getWrappedValue(" + fieldName2 + ");\n";
        setterMethodSrc2 = setterMethodSrc2 + "\tmap.put(\"" + fieldName2 + "\", val);\n";
        setterMethodSrc2 += "}\n";
        putMethodSrc = putMethodSrc + "\tif(key.equals(\"" + fieldName2 + "\")){\n";
        putMethodSrc = putMethodSrc + "\t\tset" + fieldName2.substring(0, 1).toUpperCase() + fieldName2.substring(1) + "(" + this.getMethodName(fieldType2) + ");\n";
        putMethodSrc = putMethodSrc + "\t\treturn map.put(\"" + fieldName2 + "\",val);\n";
        putMethodSrc += "\t}\n";
        final CtMethod j = CtMethod.make(setterMethodSrc2, bclass);
        bclass.addMethod(j);
        putMethodSrc += "\treturn null;\n";
        putMethodSrc += "}\n";
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("putMethodSrc : \n" + putMethodSrc));
        }
        final CtMethod putm = CtMethod.make(putMethodSrc, bclass);
        bclass.addMethod(putm);
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("bean class generated. name = " + bclass.getName() + ", toString = " + bclass));
        }
        final byte[] bytecode = bclass.toBytecode();
        this.addBundleClass(bundleUri, className, bytecode, true);
    }
    
    public void addHDContextClass(final MetaInfo.Type metaInfo) throws Exception {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        if (metaInfo == null) {
            throw new ClassNotFoundException("metaInfo is null");
        }
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("inside instantiateHDContextClass : " + metaInfo.className));
        }
        final String className = metaInfo.className + "_hdContext";
        final String appName = metaInfo.nsName;
        String bundleUri = this.getBundleUri(appName, BundleDefinition.Type.context, className);
        final BundleDefinition def = (BundleDefinition)bundles.get(bundleUri);
        if (def != null) {
            throw new IllegalArgumentException("Class for type " + className + " is already defined");
        }
        bundleUri = this.addBundleDefinition(appName, BundleDefinition.Type.context, className);
        final BundleLoader bl = this.getBundleLoader(bundleUri);
        final ClassPool pool = bl.getPool();
        final CtClass bclass = pool.makeClass(className);
        bclass.addConstructor(CtNewConstructor.defaultConstructor(bclass));
        final CtClass sclass = pool.get(HDContext.class.getName());
        SerialVersionUID.setSerialVersionUID(bclass);
        bclass.setSuperclass(sclass);
        final int noOfFields = metaInfo.fields.size();
        final CtClass[] fclasses = new CtClass[noOfFields];
        final Map<String, String> fields = metaInfo.fields;
        String putMethodSrc = "public Object put(String key, Object val) {\n";
        int i = 0;
        for (final Map.Entry<String, String> field : fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            fclasses[i] = pool.getCtClass(fieldType);
            final CtField bfield = new CtField(fclasses[i], fieldName, bclass);
            bfield.setModifiers(1);
            bclass.addField(bfield);
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("field name : " + fieldName + ", field.type : " + fieldType));
            }
            bclass.addMethod(CtNewMethod.getter("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), bfield));
            String setterMethodSrc = "public void set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + fieldType + " " + fieldName + ") {\n";
            setterMethodSrc = setterMethodSrc + "\tthis." + fieldName + " = " + fieldName + ";\n";
            setterMethodSrc = setterMethodSrc + "\tObject val = getWrappedValue(" + fieldName + ");\n";
            setterMethodSrc = setterMethodSrc + "\tmap.put(\"" + fieldName + "\", val);\n";
            setterMethodSrc += "}\n";
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("setterMethodSrc for field : " + fieldName + " \n" + setterMethodSrc));
            }
            final CtMethod m = CtMethod.make(setterMethodSrc, bclass);
            bclass.addMethod(m);
            putMethodSrc = putMethodSrc + "\tif(key.equals(\"" + fieldName + "\")){\n";
            putMethodSrc = putMethodSrc + "\t\tset" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1) + "(" + this.getMethodName(fieldType) + ");\n";
            putMethodSrc = putMethodSrc + "\t\treturn map.put(\"" + fieldName + "\",val);\n";
            putMethodSrc += "\t}\n";
            ++i;
        }
        putMethodSrc += "\treturn null;\n";
        putMethodSrc += "}\n";
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("putMethodSrc : \n" + putMethodSrc));
        }
        final CtMethod putm = CtMethod.make(putMethodSrc, bclass);
        bclass.addMethod(putm);
        if (HDLoader.logger.isTraceEnabled()) {
            HDLoader.logger.trace(("bean class generated. name = " + bclass.getName() + ", toString = " + bclass));
        }
        final byte[] bytecode = bclass.toBytecode();
        this.addBundleClass(bundleUri, className, bytecode, true);
    }
    
    public void removeAll(final String appName) throws Exception {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        final Iterator<String> it = bundles.keySet().iterator();
        while (it.hasNext()) {
            final String key = it.next();
            if (key.startsWith(appName + ":")) {
                it.remove();
            }
        }
    }
    
    public void listAllBundles() {
        final IMap<String, BundleDefinition> bundles = DistributedClassLoader.getBundles();
        final IMap<String, ClassDefinition> classes = DistributedClassLoader.getClasses();
        for (final String bundleName : bundles.keySet()) {
            System.out.println("Bundle: " + bundleName);
            final BundleDefinition def = (BundleDefinition)bundles.get(bundleName);
            for (final String className : def.getClassNames()) {
                System.out.println("  Class: " + className);
                final ClassDefinition cdef = (ClassDefinition)classes.get(className);
                System.out.println("    Code Size: " + cdef.getByteCode().length);
            }
        }
    }
    
    public void listClassAndId() {
        final IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
        for (final String className : idMap.keySet()) {
            System.out.println(className + ": " + idMap.get(className));
        }
    }
    
    public static void main(final String[] args) {
        System.out.println(Double.TYPE.getCanonicalName() + " " + Double.TYPE.getName() + " " + Double.TYPE.getSimpleName());
        final String[] array;
        final String[] names = array = new String[] { "org.joda.time.DateTime", "[Lorg.joda.time.DateTime;", "org.joda.time.DateTime[]", "[[Lorg.joda.time.DateTime;", "org.joda.time.DateTime[][]", "double", "[D", "double[]", "[[D", "double[][]" };
        for (final String name : array) {
            try {
                final Class<?> c = ClassLoader.getSystemClassLoader().loadClass(name);
                System.out.println("Classloader loaded class: " + c.getName() + " from " + name);
            }
            catch (ClassNotFoundException e) {
                System.err.println("Classloader could not load class: " + name);
            }
            try {
                final Class<?> c = Class.forName(name, false, ClassLoader.getSystemClassLoader());
                System.out.println("Forname loaded class: " + c.getName() + " from " + name);
            }
            catch (ClassNotFoundException e) {
                System.err.println("Forname could not load class: " + name);
            }
        }
    }
    
    static {
        HDLoader.logger = Logger.getLogger((Class)HDLoader.class);
    }
    
    protected static class HDLoaderDelegate extends ClassLoader
    {
        @Override
        protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
            return HDLoader.instance.loadClass(name, resolve);
        }
        
        @Override
        public Class<?> loadClass(final String name) throws ClassNotFoundException {
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("loading class " + name));
            }
            return HDLoader.instance.loadClass(name, false);
        }
        
        @Override
        public InputStream getResourceAsStream(final String name) {
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace(("getting resource " + name + " as stream."));
            }
            return HDLoader.instance.getResourceAsStream(name);
        }
    }
    
    class BundleMonitor implements EntryListener<String, BundleDefinition>
    {
        public void entryAdded(final EntryEvent<String, BundleDefinition> event) {
        }
        
        public void entryRemoved(final EntryEvent<String, BundleDefinition> event) {
            final String uri = (String)event.getKey();
            final BundleDefinition def = (BundleDefinition)event.getOldValue();
            synchronized (HDLoader.this.bundleLoaders) {
                for (final String className : def.getClassNames()) {
                    ((BridgePool)HDLoader.this.pool).removeCachedClass(className);
                    if (HDLoader.logger.isTraceEnabled()) {
                        HDLoader.logger.trace(("Removed class " + className));
                    }
                }
                final BundleLoader loader = HDLoader.this.bundleLoaders.get(uri);
                if (loader != null) {
                    loader.removeClasses();
                    HDLoader.this.bundleLoaders.remove(uri);
                }
                if (HDLoader.logger.isTraceEnabled()) {
                    HDLoader.logger.trace(("Removed bundle " + (String)event.getKey()));
                }
            }
        }
        
        public void entryUpdated(final EntryEvent<String, BundleDefinition> event) {
        }
        
        public void entryEvicted(final EntryEvent<String, BundleDefinition> event) {
        }
        
        public void mapCleared(final MapEvent arg0) {
        }
        
        public void mapEvicted(final MapEvent arg0) {
        }
    }
    
    class BridgePool extends ClassPool
    {
        HDLoader peerLoader;
        
        public BridgePool(final HDLoader bLoader) {
            super(true);
            this.peerLoader = bLoader;
        }
        
        public CtClass get(final String name) throws NotFoundException {
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace((this.peerLoader.getName() + " get CtClass " + name));
            }
            final BundleLoader bl = this.peerLoader.getBundleLoaderForClass(name);
            if (bl != null) {
                if (HDLoader.logger.isTraceEnabled()) {
                    HDLoader.logger.trace((this.peerLoader.getName() + " using BundleLoader " + bl.getName() + " for " + name));
                }
                return bl.getPool().get(name);
            }
            if (HDLoader.logger.isTraceEnabled()) {
                HDLoader.logger.trace((this.peerLoader.getName() + " using default pool for " + name));
            }
            return super.get(name);
        }
        
        public void removeCachedClass(final String classname) {
            this.removeCached(classname);
        }
    }
}
