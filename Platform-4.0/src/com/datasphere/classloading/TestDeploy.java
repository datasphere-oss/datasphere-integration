package com.datasphere.classloading;

import org.apache.log4j.*;
import java.util.*;

public class TestDeploy
{
    private static Logger logger;
    
    public static void outputBundles(final HDLoader bl) {
        for (final String bundleName : DistributedClassLoader.getBundles().keySet()) {
            if (TestDeploy.logger.isDebugEnabled()) {
                TestDeploy.logger.debug((Object)("Bundle: " + bundleName));
            }
            final BundleDefinition def = (BundleDefinition)DistributedClassLoader.getBundles().get((Object)bundleName);
            for (final String className : def.getClassNames()) {
                if (TestDeploy.logger.isDebugEnabled()) {
                    TestDeploy.logger.debug((Object)("  Class: " + className));
                }
                final ClassDefinition cdef = (ClassDefinition)DistributedClassLoader.getClasses().get((Object)className);
                if (TestDeploy.logger.isDebugEnabled()) {
                    TestDeploy.logger.debug((Object)("    Code Size: " + cdef.getByteCode().length));
                }
            }
        }
    }
    
    public static void testDeploy(final HDLoader bl, final String path, final String name) {
        try {
            bl.addJar("Global", path, name);
        }
        catch (Exception e) {
            TestDeploy.logger.error((Object)"Problem deploying jar", (Throwable)e);
        }
        outputBundles(bl);
    }
    
    public static void testUndeploy(final HDLoader bl, final String name) {
        try {
            bl.removeJar("Global", name);
        }
        catch (Exception e) {
            TestDeploy.logger.error((Object)"Problem undeploying jar", (Throwable)e);
        }
        outputBundles(bl);
    }
    
    public static void testAddType(final HDLoader bl) {
        final Map<String, String> fields = new LinkedHashMap<String, String>();
        fields.put("merchantId", "java.lang.String");
        fields.put("count", "int");
        fields.put("amount", "double");
        fields.put("other", "com.x.test.UsefulObject");
        try {
            bl.addTypeClass("MyApp", "myapp.MyClass", fields);
        }
        catch (Exception e) {
            TestDeploy.logger.error((Object)"Problem adding type", (Throwable)e);
        }
        outputBundles(bl);
    }
    
    public static void testAddType2(final HDLoader bl) {
        final Map<String, String> fields = new LinkedHashMap<String, String>();
        fields.put("event", "com.datasphere.event.Event");
        fields.put("myClass", "myapp.MyClass");
        try {
            bl.addTypeClass("MyApp", "myapp.MyOtherClass", fields);
        }
        catch (Exception e) {
            TestDeploy.logger.error((Object)"Problem adding type", (Throwable)e);
        }
        outputBundles(bl);
    }
    
    public static void testRemoveType(final HDLoader bl) {
        try {
            bl.removeTypeClass("MyApp", "myapp.MyClass");
        }
        catch (Exception e) {
            TestDeploy.logger.error((Object)"Problem removing type", (Throwable)e);
        }
        outputBundles(bl);
    }
    
    public static final void main(final String[] args) {
        final HDLoader bl = HDLoader.get(true);
        testDeploy(bl, "/Users/steve/Code/HD/LoadTestSample1/loadTestSample1.jar", "loadTestSample");
        testAddType(bl);
        testAddType2(bl);
        testUndeploy(bl, "loadTestSample");
        testRemoveType(bl);
        testAddType(bl);
        testDeploy(bl, "/Users/steve/Code/HD/LoadTestSample2/loadTestSample2.jar", "loadTestSample");
        testUndeploy(bl, "loadTestSample");
        testDeploy(bl, "/Users/steve/Code/HD/LoadTestSample2/loadTestSample2.jar", "loadTestSample");
        testDeploy(bl, "/Users/steve/Code/HD/LoadTestSample2/loadTestSample2.jar", "loadTestSample2");
        System.exit(0);
    }
    
    static {
        TestDeploy.logger = Logger.getLogger((Class)TestDeploy.class);
    }
}
