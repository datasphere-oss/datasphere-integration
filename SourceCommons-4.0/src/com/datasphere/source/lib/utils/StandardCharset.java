package com.datasphere.source.lib.utils;

import java.io.*;

public abstract class StandardCharset
{
    public abstract boolean skipBOM(final InputStream p0) throws IOException;
    
    public static StandardCharset getCharset(final String charsetName) {
        if (charsetName.equalsIgnoreCase("UTF-8")) {
            return new UTF_8();
        }
        if (charsetName.equalsIgnoreCase("UTF-16BE")) {
            return new UTF_16BE();
        }
        if (charsetName.equalsIgnoreCase("UTF-16LE")) {
            return new UTF_16LE();
        }
        if (charsetName.equalsIgnoreCase("UTF-32BE")) {
            return new UTF_32BE();
        }
        if (charsetName.equalsIgnoreCase("UTF-32LE")) {
            return new UTF_32LE();
        }
        return null;
    }
}
