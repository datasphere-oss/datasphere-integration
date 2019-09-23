package com.datasphere.runtime.compiler;

import javax.tools.*;

import com.datasphere.classloading.*;

import java.net.*;
import java.io.*;

class XJavaFileObject extends SimpleJavaFileObject
{
    public final String name;
    
    protected XJavaFileObject(final String name) {
        super(URI.create("string:///" + name.replace('.', '/') + JavaFileObject.Kind.CLASS.extension), JavaFileObject.Kind.CLASS);
        this.name = name;
    }
    
    @Override
    public InputStream openInputStream() throws IOException {
        final byte[] bytes = WALoader.get().getClassDefinition(this.name).getByteCode();
        return new ByteArrayInputStream(bytes);
    }
}
