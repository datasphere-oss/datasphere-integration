package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "array", propOrder = { "column", "temporal", "enumerated", "lob", "convert", "converterOrTypeConverterOrObjectTypeConverter", "property", "accessMethods" })
public class Array
{
    protected Column column;
    protected TemporalType temporal;
    protected EnumType enumerated;
    protected Lob lob;
    protected String convert;
    @XmlElements({ @XmlElement(name = "converter", type = Converter.class), @XmlElement(name = "type-converter", type = TypeConverter.class), @XmlElement(name = "object-type-converter", type = ObjectTypeConverter.class), @XmlElement(name = "struct-converter", type = StructConverter.class) })
    protected List<Object> converterOrTypeConverterOrObjectTypeConverter;
    protected List<Property> property;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "database-type", required = true)
    protected String databaseType;
    @XmlAttribute(name = "target-class")
    protected String targetClass;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "attribute-type")
    protected String attributeType;
    
    public Column getColumn() {
        return this.column;
    }
    
    public void setColumn(final Column value) {
        this.column = value;
    }
    
    public TemporalType getTemporal() {
        return this.temporal;
    }
    
    public void setTemporal(final TemporalType value) {
        this.temporal = value;
    }
    
    public EnumType getEnumerated() {
        return this.enumerated;
    }
    
    public void setEnumerated(final EnumType value) {
        this.enumerated = value;
    }
    
    public Lob getLob() {
        return this.lob;
    }
    
    public void setLob(final Lob value) {
        this.lob = value;
    }
    
    public String getConvert() {
        return this.convert;
    }
    
    public void setConvert(final String value) {
        this.convert = value;
    }
    
    public List<Object> getConverterOrTypeConverterOrObjectTypeConverter() {
        if (this.converterOrTypeConverterOrObjectTypeConverter == null) {
            this.converterOrTypeConverterOrObjectTypeConverter = new ArrayList<Object>();
        }
        return this.converterOrTypeConverterOrObjectTypeConverter;
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
    
    public String getDatabaseType() {
        return this.databaseType;
    }
    
    public void setDatabaseType(final String value) {
        this.databaseType = value;
    }
    
    public String getTargetClass() {
        return this.targetClass;
    }
    
    public void setTargetClass(final String value) {
        this.targetClass = value;
    }
    
    public AccessType getAccess() {
        return this.access;
    }
    
    public void setAccess(final AccessType value) {
        this.access = value;
    }
    
    public String getAttributeType() {
        return this.attributeType;
    }
    
    public void setAttributeType(final String value) {
        this.attributeType = value;
    }
}
