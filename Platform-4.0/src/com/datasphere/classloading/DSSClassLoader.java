package com.datasphere.classloading;

import java.io.*;
import java.util.*;
import java.net.*;
/*
 * 动态类加载
 */
public class DSSClassLoader extends URLClassLoader
{
    private static final boolean DEBUG = false;
    private Map<String, ModuleClassLoader> moduleClassLoaderMap;
    private HDLoader dynamicClassLoader;
    
    public DSSClassLoader(final ClassLoader parent) {
        super(((URLClassLoader)parent).getURLs(), parent);
        this.moduleClassLoaderMap = new WeakHashMap<String, ModuleClassLoader>();
        this.dynamicClassLoader = null;
    }
    
    private void addModulesFromDir(final String path) throws IOException {
        final File modulesDir = new File(path);
        final File[] moduleFiles = modulesDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(final File pathname) {
                return pathname.getName().endsWith(".scm");
            }
        });
        for (int i = 0; moduleFiles != null && i < moduleFiles.length; ++i) {
            final ModuleClassLoader mcl = new ModuleClassLoader(moduleFiles[i], this);
            this.moduleClassLoaderMap.put(mcl.getModuleServiceImplClass(), mcl);
        }
    }
    
    public void scanModulePath() throws IOException {
        final String module_dir_path = System.getProperty("dss.modules.path");
        if (module_dir_path != null) {
            final StringTokenizer tokenizer = new StringTokenizer(module_dir_path, ":");
            while (tokenizer.hasMoreTokens()) {
                this.addModulesFromDir(tokenizer.nextToken());
            }
        }
        this.dynamicClassLoader = HDLoader.get();
    }
    
    @Override
    public Class<?> loadClass(final String name) throws ClassNotFoundException {
        final ModuleClassLoader mcl = this.moduleClassLoaderMap.get(name);
        if (mcl != null) {
            return mcl.loadClass(name);
        }
        if (this.dynamicClassLoader != null) {
            return this.dynamicClassLoader.loadClass(name);
        }
        return super.loadClass(name);
    }
    
    public Set<Class<?>> getAllServicesOfModules() {
        final Set<String> classNames = this.moduleClassLoaderMap.keySet();
        final Set<Class<?>> clazzes = new HashSet<Class<?>>();
        for (final String clz : classNames) {
            try {
                clazzes.add(this.loadClass(clz));
            }
            catch (ClassNotFoundException cnfe) {
                cnfe.printStackTrace();
            }
        }
        return clazzes;
    }
    
    private void appendToClassPathForInstrumentation(final String path) {
        try {
            final URL pathUrl = new File(path).toURI().toURL();
            super.addURL(pathUrl);
        }
        catch (MalformedURLException mue) {
            System.err.println("Failed to generate URL for path : " + path);
        }
    }
}
