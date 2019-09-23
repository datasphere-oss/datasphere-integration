package com.datasphere.runtime;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.datasphere.classloading.BundleDefinition;
import com.datasphere.classloading.WALoader;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.FieldToObject;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;

public abstract class KeyFactory
{
    private static Logger logger;
    
    public abstract RecordKey makeKey(final Object p0);
    
    public static KeyFactory genKeyFactory(final String bundleUri, final WALoader wal, final String eventTypeClassName, final List<String> fieldNames) throws Exception {
        final Class<?> eventType = wal.loadClass(eventTypeClassName);
        final ClassPool pool = wal.getBundlePool(bundleUri);
        final String className = "KeyFactory" + System.nanoTime();
        final CtClass cc = pool.makeClass(className);
        final CtClass sup = pool.get(KeyFactory.class.getName());
        cc.setSuperclass(sup);
        final StringBuilder src = new StringBuilder();
        src.append("public " + RecordKey.class.getName() + " makeKey(Object obj)\n{\n");
        src.append("\t" + eventType.getName() + " tmp = (" + eventType.getName() + ")obj;\n");
        src.append("\tObject[] key = {");
        String sep = "";
        for (final String fieldName : fieldNames) {
            try {
                final Field f = NamePolicy.getField(eventType, fieldName);
                src.append(sep);
                src.append(FieldToObject.genConvert("tmp." + f.getName()));
                sep = ", ";
            }
            catch (NoSuchFieldException | SecurityException ex2) {
                KeyFactory.logger.error((Object)("Could not obtain field " + fieldName + " from " + eventTypeClassName), (Throwable)ex2);
                final Field[] fs = eventType.getDeclaredFields();
                KeyFactory.logger.error((Object)("Fields are: " + Arrays.toString(fs)));
            }
        }
        src.append("};\n");
        final String factory = RecordKey.getObjArrayKeyFactory();
        src.append("\treturn " + factory + "(key);\n");
        src.append("}\n");
        final String code = src.toString();
        final CtMethod m = CtNewMethod.make(code, cc);
        cc.addMethod(m);
        cc.setModifiers(cc.getModifiers() & 0xFFFFFBFF);
        cc.setModifiers(1);
        wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
        final Class<?> klass = wal.loadClass(className);
        final KeyFactory kf = (KeyFactory)klass.newInstance();
        return kf;
    }
    
    public static KeyFactory createKeyFactory(final MetaInfo.MetaObject obj, final List<String> fields, final UUID dataType, final BaseServer srv) throws Exception {
        if (!fields.isEmpty()) {
            final WALoader wal = WALoader.get();
            final String bundleUri = wal.createIfNotExistsBundleDefinition(obj.nsName, BundleDefinition.Type.keyFactory, obj.name);
            final MetaInfo.Type t = srv.getTypeInfo(dataType);
            return genKeyFactory(bundleUri, wal, t.className, fields);
        }
        return null;
    }
    
    public static KeyFactory createKeyFactory(final String nsName, final String name, final List<String> fields, final UUID dataType, final BaseServer srv) throws Exception {
        if (!fields.isEmpty()) {
            final WALoader wal = WALoader.get();
            final String bundleUri = wal.createIfNotExistsBundleDefinition(nsName, BundleDefinition.Type.keyFactory, name);
            final MetaInfo.Type t = srv.getTypeInfo(dataType);
            return genKeyFactory(bundleUri, wal, t.className, fields);
        }
        return null;
    }
    
    public static KeyFactory createKeyFactory(final MetaInfo.MetaObject obj, final List<String> fields, final UUID dataType) throws Exception {
        if (!fields.isEmpty()) {
            final WALoader wal = WALoader.get();
            final String bundleUri = wal.createIfNotExistsBundleDefinition(obj.nsName, BundleDefinition.Type.keyFactory, obj.name);
            final MetaInfo.Type t = (MetaInfo.Type)MetadataRepository.getINSTANCE().getMetaObjectByUUID(dataType, HSecurityManager.TOKEN);
            return genKeyFactory(bundleUri, wal, t.className, fields);
        }
        return null;
    }
    
    public static void removeKeyFactory(final MetaInfo.MetaObject obj, final List<String> fields, final UUID dataType, final BaseServer srv) throws Exception {
        if (!fields.isEmpty()) {
            final WALoader wal = WALoader.get();
            wal.removeBundle(obj.nsName, BundleDefinition.Type.keyFactory, obj.name);
        }
    }
    
    public static void removeKeyFactory(final MetaInfo.Window windowInfo, final BaseServer srv) throws Exception {
        final MetaInfo.Stream streamInfo = srv.getStreamInfo(windowInfo.stream);
        removeKeyFactory(windowInfo, windowInfo.partitioningFields, streamInfo.dataType, srv);
    }
    
    public static void removeKeyFactory(final MetaInfo.Stream streamInfo, final BaseServer srv) throws Exception {
        removeKeyFactory(streamInfo, streamInfo.partitioningFields, streamInfo.dataType, srv);
    }
    
    public static KeyFactory createKeyFactory(final MetaInfo.Stream streamInfo, final BaseServer srv) throws Exception {
        return createKeyFactory(streamInfo, streamInfo.partitioningFields, streamInfo.dataType, srv);
    }
    
    public static KeyFactory createKeyFactory(final MetaInfo.Stream streamInfo) throws Exception {
        return createKeyFactory(streamInfo, streamInfo.partitioningFields, streamInfo.dataType);
    }
    
    public static KeyFactory createKeyFactory(final MetaInfo.Window windowInfo, final BaseServer srv) throws Exception {
        final MetaInfo.Stream streamInfo = srv.getStreamInfo(windowInfo.stream);
        return createKeyFactory(windowInfo, windowInfo.partitioningFields, streamInfo.dataType, srv);
    }
    
    static {
        KeyFactory.logger = Logger.getLogger((Class)KeyFactory.class);
    }
}
