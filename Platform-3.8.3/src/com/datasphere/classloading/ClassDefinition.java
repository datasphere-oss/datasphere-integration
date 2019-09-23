package com.datasphere.classloading;

import java.io.*;

public class ClassDefinition implements Serializable
{
    private static final long serialVersionUID = 4175403813634486465L;
    private String bundleName;
    private String className;
    private int classId;
    private byte[] byteCode;
    
    public ClassDefinition() {
    }
    
    public ClassDefinition(final String bundleName, final String className, final int classId, final byte[] byteCode) {
        this.bundleName = bundleName;
        this.className = className;
        this.classId = classId;
        this.byteCode = byteCode.clone();
    }
    
    public String getBundleName() {
        return this.bundleName;
    }
    
    public void setBundleName(final String bundleName) {
        this.bundleName = bundleName;
    }
    
    public String getClassName() {
        return this.className;
    }
    
    public void setClassName(final String className) {
        this.className = className;
    }
    
    public int getClassId() {
        return this.classId;
    }
    
    public void setClassId(final int classId) {
        this.classId = classId;
    }
    
    public byte[] getByteCode() {
        return this.byteCode;
    }
    
    public void setByteCode(final byte[] byteCode) {
        this.byteCode = byteCode.clone();
    }
}
