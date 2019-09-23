package com.datasphere.preview;

import java.util.*;

public enum ParserType
{
    DSV("DSVPARSER") {
        @Override
        public String getForClassName() {
            return "DSVParser";
        }
        
        @Override
        public Map<String, Object> getDefaultParserProperties() {
            final Map<String, Object> parserProperties = new HashMap<String, Object>();
            parserProperties.put(DataPreviewImpl.MODULE_NAME, this.getForClassName());
            parserProperties.put("header", false);
            parserProperties.put("rowdelimiter", "\n:\r");
            return parserProperties;
        }
    }, 
    XML("XMLPARSER") {
        @Override
        public String getForClassName() {
            return "XMLParser";
        }
        
        @Override
        public Map<String, Object> getDefaultParserProperties() {
            final Map<String, Object> parserProperties = new HashMap<String, Object>();
            parserProperties.put(DataPreviewImpl.MODULE_NAME, this.getForClassName());
            return parserProperties;
        }
    }, 
    JSON("JSONPARSER") {
        @Override
        public String getForClassName() {
            return "JSONParser";
        }
        
        @Override
        public Map<String, Object> getDefaultParserProperties() {
            final Map<String, Object> parserProperties = new HashMap<String, Object>();
            parserProperties.put(DataPreviewImpl.MODULE_NAME, this.getForClassName());
            return parserProperties;
        }
    }, 
    FreeFormText("FreeFormTextParser") {
        @Override
        public String getForClassName() {
            return "FreeFormTextParser";
        }
        
        @Override
        public Map<String, Object> getDefaultParserProperties() {
            final Map<String, Object> parserProperties = new HashMap<String, Object>();
            parserProperties.put(DataPreviewImpl.MODULE_NAME, this.getForClassName());
            return parserProperties;
        }
    };
    
    String parserName;
    
    private ParserType(final String parserName) {
        this.parserName = null;
        this.parserName = parserName;
    }
    
    public String getParserName() {
        return this.parserName;
    }
    
    public abstract String getForClassName();
    
    public abstract Map<String, Object> getDefaultParserProperties();
}
