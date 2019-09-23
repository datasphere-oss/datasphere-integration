package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "basic-collection", propOrder = { "valueColumn", "convert", "converter", "typeConverter", "objectTypeConverter", "structConverter", "collectionTable", "joinFetch", "batchFetch", "property", "accessMethods", "noncacheable" })
public class BasicCollection
{
    @XmlElement(name = "value-column")
    protected Column valueColumn;
    protected String convert;
    protected Converter converter;
    @XmlElement(name = "type-converter")
    protected TypeConverter typeConverter;
    @XmlElement(name = "object-type-converter")
    protected ObjectTypeConverter objectTypeConverter;
    @XmlElement(name = "struct-converter")
    protected StructConverter structConverter;
    @XmlElement(name = "collection-table")
    protected EclipselinkCollectionTable collectionTable;
    @XmlElement(name = "join-fetch")
    protected JoinFetchType joinFetch;
    @XmlElement(name = "batch-fetch")
    protected BatchFetch batchFetch;
    protected List<Property> property;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    protected EmptyType noncacheable;
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "fetch")
    protected FetchType fetch;
    @XmlAttribute(name = "access")
    protected AccessType access;
    
    public Column getValueColumn() {
        return this.valueColumn;
    }
    
    public void setValueColumn(final Column value) {
        this.valueColumn = value;
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
    
    public EclipselinkCollectionTable getCollectionTable() {
        return this.collectionTable;
    }
    
    public void setCollectionTable(final EclipselinkCollectionTable value) {
        this.collectionTable = value;
    }
    
    public JoinFetchType getJoinFetch() {
        return this.joinFetch;
    }
    
    public void setJoinFetch(final JoinFetchType value) {
        this.joinFetch = value;
    }
    
    public BatchFetch getBatchFetch() {
        return this.batchFetch;
    }
    
    public void setBatchFetch(final BatchFetch value) {
        this.batchFetch = value;
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
    
    public EmptyType getNoncacheable() {
        return this.noncacheable;
    }
    
    public void setNoncacheable(final EmptyType value) {
        this.noncacheable = value;
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
    
    public AccessType getAccess() {
        return this.access;
    }
    
    public void setAccess(final AccessType value) {
        this.access = value;
    }
}
