package com.datasphere.metaRepository;

import org.apache.log4j.*;
import org.reflections.util.*;
import org.reflections.*;

import com.datasphere.anno.*;
import com.datasphere.runtime.meta.*;
import java.util.*;

public class ReflectionsTest
{
    private static Logger logger;
    
    public static void main(final String[] args) {
        final MDCache c = MDCache.getInstance();
        final Reflections refs = new Reflections(new Object[] { ClasspathHelper.forPackage("com.datasphere.proc", new ClassLoader[0]) });
        final Set<Class<?>> annotatedClasses = (Set<Class<?>>)refs.getTypesAnnotatedWith((Class)PropertyTemplate.class);
        final Map<String, MetaInfo.PropertyDef> propertyMap = new HashMap<String, MetaInfo.PropertyDef>();
        MetaInfo.PropertyTemplateInfo pti = null;
        for (final Class<?> cc : annotatedClasses) {
            final Object[] a = cc.getAnnotations();
            PropertyTemplate pt = null;
            for (final Object aa : a) {
                if (aa instanceof PropertyTemplate) {
                    pt = (PropertyTemplate)aa;
                }
            }
            final PropertyTemplateProperty[] properties;
            final PropertyTemplateProperty[] ptp = properties = pt.properties();
            for (final PropertyTemplateProperty val : properties) {
                final MetaInfo.PropertyDef def = new MetaInfo.PropertyDef();
                def.construct(val.required(), val.type(), val.defaultValue(), val.label(), val.description());
                propertyMap.put(val.name(), def);
            }
            pti = new MetaInfo.PropertyTemplateInfo();
            pti.construct(pt.name(), pt.type(), propertyMap, pt.inputType().getName(), pt.outputType().getName(), cc.getName(), pt.requiresParser(), pt.requiresFormatter(), pt.version(), pt.isParallelizable());
            if (ReflectionsTest.logger.isInfoEnabled()) {
                ReflectionsTest.logger.info((Object)(pti.name + " ; " + pti.getPropertyMap() + " ; " + pti.className));
            }
            c.put(pti);
        }
    }
    
    static {
        ReflectionsTest.logger = Logger.getLogger((Class)ReflectionsTest.class);
    }
}
