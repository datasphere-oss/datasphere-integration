package com.datasphere.classloading;

import java.net.*;
import java.io.*;
import java.util.jar.*;
import org.apache.log4j.*;

public class ModuleClassLoader extends URLClassLoader
{
    public static final String DSS_MODULE_NAME_MANIFEST_KEY = "DSS-Module-Name";
    public static final String DSS_SERVICE_IMPL_MANIFEST_KEY = "DSS-Service-Implementation";
    private String moduleName;
    private String moduleServiceImplClass;
    
    public ModuleClassLoader(final File scm, final ClassLoader parent) throws IOException {
        super(new URL[] { scm.toURL() }, parent);
        final JarFile scmFile = new JarFile(scm);
        final Manifest mf = scmFile.getManifest();
        if (mf == null) {
            throw new IOException("Invalid DSS module file with no manifest : " + scm.getPath());
        }
        final Attributes attributes = mf.getMainAttributes();
        this.moduleName = attributes.getValue("DSS-Module-Name");
        if (this.moduleName == null) {
            throw new IOException("Invalid DSS module file with no module name in manifest file :" + scm.getPath());
        }
        this.moduleServiceImplClass = attributes.getValue("DSS-Service-Implementation");
        if (this.moduleServiceImplClass == null) {
            throw new IOException("Invalid DSS module file with no service class in manifest file :" + scm.getPath());
        }
    }
    
    public String getModuleName() {
        return this.moduleName;
    }
    
    public String getModuleServiceImplClass() {
        return this.moduleServiceImplClass;
    }
    
    @Override
    public Class<?> loadClass(final String name) throws ClassNotFoundException {
        Class result = this.findLoadedClass(name);
        if (result != null) {
            return (Class<?>)result;
        }
        try {
            result = this.findClass(name);
        }
        catch (ClassNotFoundException cnfe) {
            result = super.loadClass(name);
        }
        return (Class<?>)result;
    }
    
    public static void main(final String[] args) throws Exception {
        Logger.getLogger((Class)ModuleClassLoader.class).setLevel(Level.ALL);
        final ModuleClassLoader mcl = new ModuleClassLoader(new File("/Users/theseusyang/projects/datasphere/Product/Adapters/Sources/OracleReader/target/OracleReader.scm"), Thread.currentThread().getContextClassLoader());
        Class cl = mcl.loadClass("com.datasphere.source.oraclecommon.SQL");
        System.out.println("Class is loaded fine? " + (cl != null));
        cl = mcl.loadClass("com.datasphere.source.oraclecommon.SQL");
        System.out.println("Class is loaded fine? " + (cl != null));
        cl = mcl.loadClass("com.google.protobuf.DescriptorProtos");
        System.out.println("Class is loaded fine? " + (cl != null));
        cl = mcl.loadClass("java.lang.String");
        System.out.println("Class is loaded fine? " + (cl != null));
        final URL url = mcl.findResource("proto/record.proto");
        System.out.println("Resource is loaded fine? " + (url != null));
    }
}
