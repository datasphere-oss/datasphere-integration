package com.datasphere.source.lib.utils;

import java.lang.reflect.*;
import java.util.*;
import com.datasphere.common.exc.*;

public class XMLElementBuilder
{
    private Map<String, Object> elementMap;
    
    public XMLElementBuilder(final Map<String, Object> xmlProperties, final Field[] fields) throws AdapterException {
        this.elementMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        this.initializeMap(xmlProperties, fields);
    }
    
    private void initializeMap(final Map<String, Object> xmlProperties, final Field[] fields) throws AdapterException {
        final String elementTuple = (String)xmlProperties.get("elementtuple");
        final String[] split;
        final String[] elementTupleArray = split = elementTuple.split(",");
        for (final String element : split) {
            final Element et = new Element(element, fields);
            this.elementMap.put(et.elementName, et);
        }
    }
    
    public Map<String, Object> getElementMap() {
        return this.elementMap;
    }
    
    public class Element
    {
        private String elementName;
        private Map<String, Field> attributeFieldMap;
        private Field textField;
        
        public Element(final String elementTuple, final Field[] fields) throws AdapterException {
            this.textField = null;
            this.attributeFieldMap = new TreeMap<String, Field>(String.CASE_INSENSITIVE_ORDER);
            final String[] elementTupleArray = elementTuple.split(":");
            final int length = elementTupleArray.length;
            this.elementName = elementTupleArray[0];
            if (length > 1) {
                final String[] text = elementTupleArray[length - 1].split("=");
                if (text.length == 2 && !text[1].trim().isEmpty()) {
                    this.textField = this.iterateOverFields(fields, text[1]);
                }
                for (int i = 1; i < length - 1; ++i) {
                    final String attributeName = elementTupleArray[i];
                    this.attributeFieldMap.put(attributeName, this.iterateOverFields(fields, attributeName));
                }
            }
        }
        
        private Field iterateOverFields(final Field[] fields, final String fieldName) throws AdapterException {
            Field fieldNameToBeReturned = null;
            for (final Field field : fields) {
                if (field.getName().equalsIgnoreCase(fieldName)) {
                    fieldNameToBeReturned = field;
                    break;
                }
            }
            if (fieldNameToBeReturned == null) {
                throw new AdapterException("Attribute or Text dooesn't have a field/type associated with it");
            }
            return fieldNameToBeReturned;
        }
        
        public String getElementName() {
            return this.elementName;
        }
        
        public Map<String, Field> getAttributeFieldMap() {
            return this.attributeFieldMap;
        }
        
        public String processTextValue(final Object event) throws Exception {
            if (this.textField != null) {
                return this.textField.get(event).toString();
            }
            return "";
        }
    }
}
