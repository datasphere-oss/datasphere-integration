package com.datasphere.preview;

import java.util.*;

public enum ReaderType
{
    FileReader {
        @Override
        public Map<String, Object> getDefaultReaderProperties() {
            final Map<String, Object> readerProperties = new HashMap<String, Object>();
            readerProperties.put("blockSize", 64);
            readerProperties.put("positionbyeof", false);
            readerProperties.put("charset", "UTF-8");
            return readerProperties;
        }
        
        @Override
        public Map<String, String> getDefaultReaderPropertiesAsString() {
            final Map<String, String> readerProperties = new HashMap<String, String>();
            readerProperties.put("blockSize", "64");
            readerProperties.put("positionbyeof", "false");
            readerProperties.put("charset", "UTF-8");
            return readerProperties;
        }
    }, 
    HDFSReader {
        @Override
        public Map<String, Object> getDefaultReaderProperties() {
            final Map<String, Object> readerProperties = new HashMap<String, Object>();
            readerProperties.put("positionByEOF", false);
            readerProperties.put("charset", "UTF-8");
            return readerProperties;
        }
        
        @Override
        public Map<String, String> getDefaultReaderPropertiesAsString() {
            final Map<String, String> readerProperties = new HashMap<String, String>();
            readerProperties.put("positionbyeof", "false");
            readerProperties.put("charset", "UTF-8");
            return readerProperties;
        }
    };
    
    public abstract Map<String, Object> getDefaultReaderProperties();
    
    public abstract Map<String, String> getDefaultReaderPropertiesAsString();
}
