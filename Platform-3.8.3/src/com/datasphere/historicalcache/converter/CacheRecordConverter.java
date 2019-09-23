package com.datasphere.historicalcache.converter;

import org.apache.log4j.*;

import java.util.*;
import javassist.*;
import java.io.*;

import com.datasphere.errorhandling.*;
import com.datasphere.classloading.*;
import com.datasphere.event.*;
import com.datasphere.common.errors.*;
import org.joda.time.*;
import com.datasphere.runtime.*;

public class CacheRecordConverter
{
    private static final Logger logger;
    private static final String SEPARATOR = "_";
    private static final String INSTANCE = "INSTANCE";
    private final String namespaceName;
    private final String cacheName;
    private final Class typeClass;
    private final WALoader classLoader;
    private final String className;
    private String keyField;
    private String bundleURI;
    
    public CacheRecordConverter(final WALoader classLoader, final String namespaceName, final String cacheName, final Class typeClass, final String keyField) {
        this.bundleURI = null;
        if (classLoader == null || namespaceName == null || cacheName == null || typeClass == null) {
            throw new IllegalArgumentException("Missing parameter passed");
        }
        this.namespaceName = namespaceName;
        this.typeClass = typeClass;
        this.cacheName = cacheName;
        this.classLoader = classLoader;
        this.className = cacheName + "_" + typeClass.getSimpleName();
        this.keyField = keyField;
    }
    
    public Class<?> generateCacheRecordConverter(final Map<String, String> fields) throws NotFoundException, CannotCompileException, ClassNotFoundException, IOException {
        Class<?> converterClass = null;
        String bundleURI = this.classLoader.getBundleUri(this.namespaceName, BundleDefinition.Type.recordConverter, this.className);
        this.classLoader.lockClass(bundleURI);
        try {
            if (this.classLoader.isExistingBundle(this.classLoader.getBundleUri(this.namespaceName, BundleDefinition.Type.recordConverter, this.className))) {
                converterClass = this.classLoader.loadClass(makeClassName(this.namespaceName, this.cacheName, this.typeClass));
            }
            else {
                bundleURI = this.classLoader.createIfNotExistsBundleDefinition(this.namespaceName, BundleDefinition.Type.recordConverter, this.className);
                final ClassPool pool = this.classLoader.getBundlePool(bundleURI);
                final CtClass baseClass = createClass(pool, this.namespaceName, this.cacheName, this.typeClass);
                final CtClass interfaceClass = pool.get(DataExtractor.class.getCanonicalName());
                final CtClass[] interfaceList = { interfaceClass };
                baseClass.setInterfaces(interfaceList);
                final Set<String> keySet = fields.keySet();
                for (final String key : keySet) {
                    if (key.equalsIgnoreCase(this.keyField)) {
                        this.keyField = key;
                        break;
                    }
                }
                final CtMethod m = CtMethod.make("public " + Pair.class.getCanonicalName() + " convertToObject(Object[] data) { " + this.fieldSetter(this.keyField, this.typeClass, fields) + " } ", baseClass);
                baseClass.addMethod(m);
                this.classLoader.addBundleClass(bundleURI, makeClassName(this.namespaceName, this.cacheName, this.typeClass), baseClass.toBytecode(), false);
                converterClass = this.classLoader.loadClass(makeClassName(this.namespaceName, this.cacheName, this.typeClass));
            }
        }
        catch (Exception e) {
            CacheRecordConverter.logger.warn((Object)e.getMessage(), (Throwable)e);
            if (CacheRecordConverter.logger.isDebugEnabled()) {
                CacheRecordConverter.logger.debug((Object)e.getMessage(), (Throwable)e);
            }
        }
        finally {
            this.classLoader.unlockClass(bundleURI);
        }
        return converterClass;
    }
    
    protected static String makeClassName(final String namespaceName, final String cacheName, final Class typeName) {
        return namespaceName + "_" + cacheName + "_" + typeName.getSimpleName() + "_" + "TypeConverter";
    }
    
    private static CtClass createClass(final ClassPool pool, final String namespaceName, final String cacheName, final Class typeName) {
        return pool.makeClass(makeClassName(namespaceName, cacheName, typeName));
    }
    
    private String fieldSetter(final String keyField, final Class clazz, final Map<String, String> fields) throws DatallException {
        String base = clazz.getCanonicalName() + " " + "INSTANCE" + " = ((" + clazz.getCanonicalName() + ")" + clazz.getCanonicalName() + ".class.newInstance()); \n";
        Object obj = null;
        try {
            obj = clazz.newInstance();
            if (obj instanceof SimpleEvent) {
                base = base + "((" + SimpleEvent.class.getCanonicalName() + ")" + "INSTANCE" + ").init(System.currentTimeMillis()); \n";
            }
        }
        catch (InstantiationException e) {
            throw new DatallException((IError)CacheError.CACHE_RECORD_EXTRACTOR_CANNOT_INSTANTIATE, (Throwable)e, this.namespaceName + "." + this.cacheName, (String[])null);
        }
        catch (IllegalAccessException e2) {
            throw new DatallException((IError)CacheError.CACHE_RECORD_EXTRACTOR_CANNOT_ACCESS, (Throwable)e2, this.namespaceName + "." + this.cacheName, (String[])null);
        }
        int COUNTER = 0;
        for (final Map.Entry<String, String> f : fields.entrySet()) {
            base = base + "if(data.length > " + COUNTER + ") { \n \t";
            base = base + "if(data[" + COUNTER + "] != null) { \n \t\t";
            if (f.getValue().equalsIgnoreCase("int")) {
                base = base + "INSTANCE." + f.getKey() + " = (new java.lang.Integer(data[" + COUNTER + "].toString())).intValue(); \n";
            }
            else if (f.getValue().equals("java.lang.String")) {
                base = base + "INSTANCE." + f.getKey() + " = data[" + COUNTER + "].toString(); \n";
            }
            else if (f.getValue().equalsIgnoreCase(DateTime.class.getCanonicalName())) {
                base = base + "INSTANCE." + f.getKey() + " = " + BuiltInFunc.class.getCanonicalName() + ".TO_DATE(data[" + COUNTER + "].toString()); \n";
            }
            else {
                base = base + "INSTANCE." + f.getKey() + " = new " + f.getValue() + "(data[" + COUNTER + "].toString()); \n";
            }
            base += "\t } \n";
            base += "\t else { \n ";
            base += "\t  } \n ";
            base += "  } \n";
            ++COUNTER;
        }
        if (keyField == null) {
            base = base + "return new " + Pair.class.getCanonicalName() + "(null," + "INSTANCE" + ");";
        }
        else if (fields.get(keyField).equalsIgnoreCase("int")) {
            base = base + "return new " + Pair.class.getCanonicalName() + "(new java.lang.Integer(" + "INSTANCE" + "." + keyField + ")," + "INSTANCE" + ");";
        }
        else {
            base = base + "return new " + Pair.class.getCanonicalName() + "(" + "INSTANCE" + "." + keyField + "," + "INSTANCE" + ");";
        }
        if (CacheRecordConverter.logger.isDebugEnabled()) {
            CacheRecordConverter.logger.debug((Object)base);
        }
        return base;
    }
    
    public void removeCacheRecordConverter() {
        this.bundleURI = this.classLoader.getBundleUri(this.namespaceName, BundleDefinition.Type.recordConverter, this.className);
        if (this.bundleURI != null) {
            this.classLoader.lockClass(this.className);
            try {
                this.classLoader.removeBundle(this.bundleURI);
            }
            catch (Exception e) {
                if (CacheRecordConverter.logger.isInfoEnabled()) {
                    CacheRecordConverter.logger.info((Object)"Problem removing class for existing record converter: ", (Throwable)e);
                }
            }
            finally {
                this.classLoader.unlockClass(this.className);
            }
        }
    }
    
    static {
        logger = Logger.getLogger((Class)CacheRecordConverter.class);
    }
}
