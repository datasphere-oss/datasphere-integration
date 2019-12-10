package com.datasphere.runtime.compiler;

import javax.tools.*;

import com.datasphere.classloading.*;

import java.util.*;
import java.io.*;

class XJavaFileManager extends ForwardingJavaFileManager<JavaFileManager>
{
    protected XJavaFileManager(final JavaFileManager fileManager) {
        super(fileManager);
    }
    
    @Override
    public String inferBinaryName(final JavaFileManager.Location location, final JavaFileObject file) {
        if (file instanceof XJavaFileObject) {
            return ((XJavaFileObject)file).name;
        }
        return super.inferBinaryName(location, file);
    }
    
    @Override
    public ClassLoader getClassLoader(final JavaFileManager.Location location) {
        return HDLoader.get();
    }
    
    @Override
    public Iterable<JavaFileObject> list(final JavaFileManager.Location location, final String packageName, final Set<JavaFileObject.Kind> kinds, final boolean recurse) throws IOException {
        final Iterable<JavaFileObject> stdResults = this.fileManager.list(location, packageName, kinds, recurse);
        if (location != StandardLocation.CLASS_PATH || !kinds.contains(JavaFileObject.Kind.CLASS) || !packageName.startsWith("wa")) {
            return stdResults;
        }
        final List<JavaFileObject> additional = new ArrayList<JavaFileObject>();
        for (final String name : DistributedClassLoader.getClasses().keySet()) {
            if (name.startsWith(packageName + ".")) {
                additional.add(new XJavaFileObject(name));
            }
        }
        if (additional.isEmpty()) {
            return stdResults;
        }
        for (final JavaFileObject obj : stdResults) {
            additional.add(obj);
        }
        return additional;
    }
}
