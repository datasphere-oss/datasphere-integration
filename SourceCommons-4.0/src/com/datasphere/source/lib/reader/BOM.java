package com.datasphere.source.lib.reader;

import org.apache.log4j.*;
import java.nio.*;
import java.io.*;
import java.util.*;

class BOM
{
    private static Logger logger;
    protected byte[] pattern;
    protected byte[] input;
    private static Map<String, BOM> bomClassMap;
    
    static synchronized void init() {
        if (BOM.bomClassMap == null) {
            (BOM.bomClassMap = new HashMap<String, BOM>()).put("UTF-8", new UTF8Bom());
            BOM.bomClassMap.put("UTF-16BE", new UTF16BEBom());
            BOM.bomClassMap.put("UTF-16LE", new UTF16LEBom());
            BOM.bomClassMap.put("UTF-32BE", new UTF32BEBom());
            BOM.bomClassMap.put("UTF-32LE", new UTF32LEBom());
        }
    }
    
    static void skip(final String charset, final ByteBuffer buffer) throws IOException {
        String charSet = null;
        if (charset == null || charset.isEmpty()) {
            charSet = "UTF-8";
        }
        else {
            charSet = charset;
        }
        final BOM tmp = BOM.bomClassMap.get(charSet.toUpperCase());
        if (tmp != null) {
            tmp.skipBOM(buffer);
        }
        else if (BOM.logger.isDebugEnabled()) {
            BOM.logger.debug((Object)("Unrecoganized charset [" + charset + "] ignoring BOM"));
        }
    }
    
    public void skipBOM(final ByteBuffer in) throws IOException {
        this.input = new byte[this.pattern.length];
        System.arraycopy(in.array(), 0, this.input, 0, this.pattern.length);
        if (Arrays.equals(this.input, this.pattern)) {
            in.position(this.pattern.length);
        }
    }
    
    static {
        BOM.logger = Logger.getLogger((Class)BOM.class);
    }
}
