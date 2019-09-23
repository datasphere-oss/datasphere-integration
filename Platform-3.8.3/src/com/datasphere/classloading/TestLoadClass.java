package com.datasphere.classloading;

import org.apache.log4j.*;
import javassist.*;
import java.lang.reflect.*;

public class TestLoadClass
{
    private static Logger logger;
    
    public static void testLoadCreateClass(final WALoader bl) {
        try {
            System.out.println("Get javassist class com.x.test.ObjectFactory");
            final CtClass poolClass = bl.pool.get("com.x.test.ObjectFactory");
            System.out.println("Got javassist class com.x.test.ObjectFactory " + poolClass);
            System.out.println("Get class com.x.test.ObjectFactory");
            final Class<?> clazz = bl.loadClass("com.x.test.ObjectFactory");
            if (clazz == null) {
                return;
            }
            System.out.println("Create new instance of com.x.test.ObjectFactory");
            final Object objectFactory = clazz.newInstance();
            System.out.println("Get getUsefulObject method");
            final Method getUsefulObject = clazz.getMethod("getUsefulObject", null);
            if (getUsefulObject == null) {
                return;
            }
            System.out.println("Invoke getUsefulObject method");
            final Object usefulObject = getUsefulObject.invoke(objectFactory, null);
            System.out.println("Got UsefulObject " + usefulObject);
            if (usefulObject == null) {
                return;
            }
            final Method getVersion = usefulObject.getClass().getMethod("getVersion", null);
            if (getVersion == null) {
                return;
            }
            System.out.println("Invoke getVersion method");
            final Object version = getVersion.invoke(usefulObject, null);
            System.out.println("Got version " + version);
        }
        catch (Exception e) {
            TestLoadClass.logger.error((Object)"Problem loading and creating object", (Throwable)e);
        }
    }
    
    public static final void main(final String[] args) {
        final WALoader bl = WALoader.get(true);
        testLoadCreateClass(bl);
        testLoadCreateClass(bl);
        testLoadCreateClass(bl);
        testLoadCreateClass(bl);
        System.exit(0);
    }
    
    static {
        TestLoadClass.logger = Logger.getLogger((Class)TestDeploy.class);
    }
}
