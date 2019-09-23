package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "version", propOrder = { "column", "index", "temporal", "convert", "converter", "typeConverter", "objectTypeConverter", "structConverter", "property", "accessMethods" })
public class Version
{
    protected Column column;
    protected Index index;
    protected TemporalType temporal;
    protected String convert;
    protected Converter converter;
    @XmlElement(name = "type-converter")
    protected TypeConverter typeConverter;
    @XmlElement(name = "object-type-converter")
    protected ObjectTypeConverter objectTypeConverter;
    @XmlElement(name = "struct-converter")
    protected StructConverter structConverter;
    protected List<Property> property;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "mutable")
    protected Boolean mutable;
    @XmlAttribute(name = "attribute-type")
    protected String attributeType;
    
    public Column getColumn() {
        return this.column;
    }
    
    public void setColumn(final Column value) {
        this.column = value;
    }
    
    public Index getIndex() {
        return this.index;
    }
    
    public void setIndex(final Index value) {
        this.index = value;
    }
    
    public TemporalType getTemporal() {
        return this.temporal;
    }
    
    public void setTemporal(final TemporalType value) {
        this.temporal = value;
    }
    
    public String getConvert() {
        return this.convert;
    }
    
    public void setConvert(final String value) {
        this.convert = value;
    }
    
    public Converter getConverter() {
        return this.converter;
    }
    
    public void setConverter(final Converter value) {
        this.converter = value;
    }
    
    public TypeConverter getTypeConverter() {
        return this.typeConverter;
    }
    
    public void setTypeConverter(final TypeConverter value) {
        this.typeConverter = value;
    }
    
    public ObjectTypeConverter getObjectTypeConverter() {
        return this.objectTypeConverter;
    }
    
    public void setObjectTypeConverter(final ObjectTypeConverter value) {
        this.objectTypeConverter = value;
    }
    
    public StructConverter getStructConverter() {
        return this.structConverter;
    }
    
    public void setStructConverter(final StructConverter value) {
        this.structConverter = value;
    }
    
    public List<Property> getProperty() {
        if (this.property == null) {
            this.property = new ArrayList<Property>();
        }
        return this.property;
    }
    
    public AccessMethods getAccessMethods() {
        return this.accessMethods;
    }
    
    public void setAccessMethods(final AccessMethods value) {
        this.accessMethods = value;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public AccessType getAccess() {
        return this.access;
    }
    
    public void setAccess(final AccessType value) {
        this.access = value;
    }
    
    public Boolean isMutable() {
        return this.mutable;
    }
    
    public void setMutable(final Boolean value) {
        this.mutable = value;
    }
    
    public String getAttributeType() {
        return this.attributeType;
    }
    
    public void setAttributeType(final String value) {
        this.attributeType = value;
    }
}
