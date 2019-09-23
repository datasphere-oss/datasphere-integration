package com.datasphere.classloading;

import java.io.*;
import java.util.*;

public class BundleDefinition implements Serializable
{
    private static final long serialVersionUID = 2761117186639790656L;
    private String jarPath;
    private String name;
    private String appName;
    private Type type;
    private List<String> classNames;
    
    public BundleDefinition(final Type type, final String appName, final String jarPath, final String name) {
        this.type = type;
        this.appName = appName;
        this.jarPath = jarPath;
        this.name = name;
        this.classNames = new ArrayList<String>();
    }
    
    public BundleDefinition(final Type type, final String appName, final String name) {
        this(type, appName, null, name);
    }
    
    public BundleDefinition() {
        this(Type.undefined, null, null, null);
    }
    
    public void addClassName(final String className) {
        if (this.classNames.contains(className)) {
            throw new IllegalArgumentException("Class " + className + " is already present in " + this.name);
        }
        this.classNames.add(className);
    }
    
    public String getJarPath() {
        return this.jarPath;
    }
    
    public void setJarPath(final String jarPath) {
        this.jarPath = jarPath;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public String getAppName() {
        return this.appName;
    }
    
    public void setAppName(final String appName) {
        this.appName = appName;
    }
    
    public Type getType() {
        return this.type;
    }
    
    public void setType(final Type type) {
        this.type = type;
    }
    
    public List<String> getClassNames() {
        return this.classNames;
    }
    
    public void setClassNames(final List<String> classNames) {
        this.classNames = classNames;
    }
    
    public static String getUri(final String appName, final Type type, final String name) {
        return appName + ":" + type + ":" + name;
    }
    
    public String getUri() {
        return getUri(this.appName, this.type, this.name);
    }
    
    public enum Type
    {
        undefined, 
        jar, 
        type, 
        context, 
        query, 
        queryTask, 
        keyFactory, 
        fieldFactory, 
        hd, 
        recordConverter, 
        attrExtractor;
    }
}
