package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "basic", propOrder = { "column", "field", "index", "cacheIndex", "generatedValue", "lob", "temporal", "enumerated", "convert", "converter", "typeConverter", "objectTypeConverter", "structConverter", "tableGenerator", "sequenceGenerator", "uuidGenerator", "property", "accessMethods", "returnInsert", "returnUpdate" })
public class Basic
{
    protected Column column;
    protected Field field;
    protected Index index;
    @XmlElement(name = "cache-index")
    protected CacheIndex cacheIndex;
    @XmlElement(name = "generated-value")
    protected GeneratedValue generatedValue;
    protected Lob lob;
    protected TemporalType temporal;
    protected EnumType enumerated;
    protected String convert;
    protected Converter converter;
    @XmlElement(name = "type-converter")
    protected TypeConverter typeConverter;
    @XmlElement(name = "object-type-converter")
    protected ObjectTypeConverter objectTypeConverter;
    @XmlElement(name = "struct-converter")
    protected StructConverter structConverter;
    @XmlElement(name = "table-generator")
    protected TableGenerator tableGenerator;
    @XmlElement(name = "sequence-generator")
    protected SequenceGenerator sequenceGenerator;
    @XmlElement(name = "uuid-generator")
    protected UuidGenerator uuidGenerator;
    protected List<Property> property;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    @XmlElement(name = "return-insert")
    protected ReturnInsert returnInsert;
    @XmlElement(name = "return-update")
    protected EmptyType returnUpdate;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "fetch")
    protected FetchType fetch;
    @XmlAttribute(name = "optional")
    protected Boolean optional;
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
    
    public Field getField() {
        return this.field;
    }
    
    public void setField(final Field value) {
        this.field = value;
    }
    
    public Index getIndex() {
        return this.index;
    }
    
    public void setIndex(final Index value) {
        this.index = value;
    }
    
    public CacheIndex getCacheIndex() {
        return this.cacheIndex;
    }
    
    public void setCacheIndex(final CacheIndex value) {
        this.cacheIndex = value;
    }
    
    public GeneratedValue getGeneratedValue() {
        return this.generatedValue;
    }
    
    public void setGeneratedValue(final GeneratedValue value) {
        this.generatedValue = value;
    }
    
    public Lob getLob() {
        return this.lob;
    }
    
    public void setLob(final Lob value) {
        this.lob = value;
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
    
    public TableGenerator getTableGenerator() {
        return this.tableGenerator;
    }
    
    public void setTableGenerator(final TableGenerator value) {
        this.tableGenerator = value;
    }
    
    public SequenceGenerator getSequenceGenerator() {
        return this.sequenceGenerator;
    }
    
    public void setSequenceGenerator(final SequenceGenerator value) {
        this.sequenceGenerator = value;
    }
    
    public UuidGenerator getUuidGenerator() {
        return this.uuidGenerator;
    }
    
    public void setUuidGenerator(final UuidGenerator value) {
        this.uuidGenerator = value;
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
    
    public ReturnInsert getReturnInsert() {
        return this.returnInsert;
    }
    
    public void setReturnInsert(final ReturnInsert value) {
        this.returnInsert = value;
    }
    
    public EmptyType getReturnUpdate() {
        return this.returnUpdate;
    }
    
    public void setReturnUpdate(final EmptyType value) {
        this.returnUpdate = value;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public FetchType getFetch() {
        return this.fetch;
    }
    
    public void setFetch(final FetchType value) {
        this.fetch = value;
    }
    
    public Boolean isOptional() {
        return this.optional;
    }
    
    public void setOptional(final Boolean value) {
        this.optional = value;
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
