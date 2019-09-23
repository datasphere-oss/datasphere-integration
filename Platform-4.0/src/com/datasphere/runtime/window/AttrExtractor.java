package com.datasphere.runtime.window;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;

import org.joda.time.DateTime;

import com.datasphere.classloading.BundleDefinition;
import com.datasphere.classloading.WALoader;
import com.datasphere.exception.SecurityException;
import com.datasphere.exception.ServerException;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.IntervalPolicy;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.NamePolicy;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public abstract class AttrExtractor
{
    public abstract Long getAttr(final Object p0);
    
    private static Pair<AttrExtractor, String> genAttrExtractor(final MetaInfo.Window wi, final String fieldName, final BaseServer srv) throws ServerException, ClassNotFoundException, NotFoundException, CannotCompileException, NoSuchFieldException, SecurityException, IOException, InstantiationException, IllegalAccessException, MetaDataRepositoryException {
        final WALoader wal = WALoader.get();
        final String bundleUri = wal.createIfNotExistsBundleDefinition(wi.nsName, BundleDefinition.Type.attrExtractor, wi.name);
        final MetaInfo.Stream si = srv.getStreamInfo(wi.stream);
        final MetaInfo.Type t = srv.getTypeInfo(si.dataType);
        final String eventTypeClassName = t.className;
        final Class<?> eventType = wal.loadClass(eventTypeClassName);
        final ClassPool pool = wal.getBundlePool(bundleUri);
        final String className = "AttrExtractor" + System.nanoTime();
        final CtClass cc = pool.makeClass(className);
        final CtClass sup = pool.get(AttrExtractor.class.getName());
        cc.setSuperclass(sup);
        final Field field = NamePolicy.getField(eventType, fieldName);
        final Class<?> fieldType = field.getType();
        final StringBuilder src = new StringBuilder();
        src.append("public Long getAttr(Object rec)\n{\n");
        src.append("\t" + fieldType.getName() + " tmp = ((" + eventType.getName() + ")rec)." + field.getName() + ";\n");
        if (DateTime.class.isAssignableFrom(fieldType)) {
            src.append("\tif(tmp == null) return null;\n");
            src.append("\treturn Long.valueOf(tmp.getMillis());\n");
        }
        else if (Date.class.isAssignableFrom(fieldType)) {
            src.append("\tif(tmp == null) return null;\n");
            src.append("\treturn Long.valueOf(tmp.getTime());\n");
        }
        else if (Number.class.isAssignableFrom(fieldType)) {
            src.append("\tif(tmp == null) return null;\n");
            src.append("\treturn Long.valueOf(tmp.longValue());\n");
        }
        else {
            if (fieldType != Long.TYPE && fieldType != Integer.TYPE && fieldType != Short.TYPE && fieldType != Integer.TYPE) {
                throw new RuntimeException("Cannot create a window over " + fieldName + ": " + fieldType.getName() + " field");
            }
            src.append("\treturn Long.valueOf((long)tmp);\n");
        }
        src.append("};\n");
        final String code = src.toString();
        final CtMethod m = CtNewMethod.make(code, cc);
        cc.addMethod(m);
        cc.setModifiers(cc.getModifiers() & 0xFFFFFBFF);
        cc.setModifiers(1);
        wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
        final Class<?> klass = wal.loadClass(className);
        final AttrExtractor ae = (AttrExtractor)klass.newInstance();
        return Pair.make(ae, field.getName());
    }
    
    public static CmpAttrs createAttrComparator(final String fieldName, final long interval, final MetaInfo.Window wi, final BaseServer srv) {
        try {
            final Pair<AttrExtractor, String> extAndField = genAttrExtractor(wi, fieldName, srv);
            final AttrExtractor ae = extAndField.first;
            final String keyFieldName = extAndField.second;
            return new CmpAttrs() {
                @Override
                public boolean inRange(final DARecord first, final DARecord last) {
                    final Long firstVal = ae.getAttr(first.data);
                    final Long lastVal = ae.getAttr(last.data);
                    if (lastVal == null) {
                        throw new NullPointerException("NULL key (" + keyFieldName + ") in attribute based window [" + wi.getFullName() + "]\n in event " + last.data);
                    }
                    return firstVal + interval > lastVal;
                }
            };
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public static CmpAttrs createAttrComparator(final MetaInfo.Window wi, final BaseServer srv) {
        final IntervalPolicy.AttrBasedPolicy p = wi.windowLen.getAttrPolicy();
        final String fieldName = p.getAttrName();
        final long interval = p.getAttrValueRange() / 1000L;
        return createAttrComparator(fieldName, interval, wi, srv);
    }
    
    public static void removeAttrExtractor(final MetaInfo.Window wi) {
        final WALoader wal = WALoader.get();
        wal.removeBundle(wi.nsName, BundleDefinition.Type.attrExtractor, wi.name);
    }
}
