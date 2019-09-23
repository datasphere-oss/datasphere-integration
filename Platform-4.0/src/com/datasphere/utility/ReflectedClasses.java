package com.datasphere.utility;

import org.apache.log4j.*;
import java.util.*;
import java.lang.reflect.*;
import com.datasphere.web.*;

public class ReflectedClasses
{
    private static ReflectedClasses instance;
    private Map<String, Map<String, Method>> classMap;
    private Map<String, Object> objectMap;
    private static Logger logger;
    
    public static ReflectedClasses getInstance() {
        return ReflectedClasses.instance;
    }
    
    private ReflectedClasses() {
        this.classMap = new HashMap<String, Map<String, Method>>();
        this.objectMap = new HashMap<String, Object>();
    }
    
    public Map<String, Map<String, Method>> getClassMap() {
        return this.classMap;
    }
    
    public void setClassMap(final Map<String, Map<String, Method>> classMap) {
        this.classMap = classMap;
    }
    
    public Map<String, Object> getObjectMap() {
        return this.objectMap;
    }
    
    public void setObjectMap(final Map<String, Object> objectMap) {
        this.objectMap = objectMap;
    }
    
    public void register(final Object anyObject) throws RMIWebSocketException {
        final Class<?> anyClass = anyObject.getClass();
        final String className = anyClass.getCanonicalName();
        this.objectMap.put(className, anyObject);
        final Map<String, Method> methodMap = new HashMap<String, Method>();
        this.classMap.put(className, methodMap);
        if (ReflectedClasses.logger.isInfoEnabled()) {
            ReflectedClasses.logger.info((Object)("Registering class " + className));
        }
        final Method[] declaredMethods;
        final Method[] allMethods = declaredMethods = anyClass.getDeclaredMethods();
        for (final Method method : declaredMethods) {
            if (Modifier.isPublic(method.getModifiers())) {
                final String methodName = method.getName();
                final Class<?>[] types = method.getParameterTypes();
                final String methodKey = methodName + ":" + types.length;
                if (methodMap.containsKey(methodKey)) {
                    throw new RMIWebSocketException("Duplicate method with name " + methodName + " and parameter count " + method.getParameterTypes().length + " defined in " + className);
                }
                if (ReflectedClasses.logger.isInfoEnabled()) {
                    ReflectedClasses.logger.info((Object)("Added method " + methodKey));
                }
                methodMap.put(methodKey, method);
            }
        }
    }
    
    public Map<String, Method> getMethodsForClass(final String className) {
        return this.classMap.get(className);
    }
    
    public Object getInstanceForClass(final String className) {
        return this.objectMap.get(className);
    }
    
    public Method getMethod(final String className, final String methodKey) {
        return this.classMap.get(className).get(methodKey);
    }
    
    static {
        ReflectedClasses.instance = new ReflectedClasses();
        ReflectedClasses.logger = Logger.getLogger((Class)ReflectedClasses.class);
    }
}
