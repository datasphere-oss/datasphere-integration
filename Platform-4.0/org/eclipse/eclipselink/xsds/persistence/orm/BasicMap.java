package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "basic-map", propOrder = { "keyColumn", "keyConverter", "valueColumn", "valueConverter", "converterOrTypeConverterOrObjectTypeConverter", "collectionTable", "joinFetch", "batchFetch", "property", "accessMethods", "noncacheable" })
public class BasicMap
{
    @XmlElement(name = "key-column")
    protected Column keyColumn;
    @XmlElement(name = "key-converter")
    protected String keyConverter;
    @XmlElement(name = "value-column")
    protected Column valueColumn;
    @XmlElement(name = "value-converter")
    protected String valueConverter;
    @XmlElements({ @XmlElement(name = "converter", type = Converter.class), @XmlElement(name = "type-converter", type = TypeConverter.class), @XmlElement(name = "object-type-converter", type = ObjectTypeConverter.class), @XmlElement(name = "struct-converter", type = StructConverter.class) })
    protected List<Object> converterOrTypeConverterOrObjectTypeConverter;
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
    
    public Column getKeyColumn() {
        return this.keyColumn;
    }
    
    public void setKeyColumn(final Column value) {
        this.keyColumn = value;
    }
    
    public String getKeyConverter() {
        return this.keyConverter;
    }
    
    public void setKeyConverter(final String value) {
        this.keyConverter = value;
    }
    
    public Column getValueColumn() {
        return this.valueColumn;
    }
    
    public void setValueColumn(final Column value) {
        this.valueColumn = value;
    }
    
    public String getValueConverter() {
        return this.valueConverter;
    }
    
    public void setValueConverter(final String value) {
        this.valueConverter = value;
    }
    
    public List<Object> getConverterOrTypeConverterOrObjectTypeConverter() {
        if (this.converterOrTypeConverterOrObjectTypeConverter == null) {
            this.converterOrTypeConverterOrObjectTypeConverter = new ArrayList<Object>();
        }
        return this.converterOrTypeConverterOrObjectTypeConverter;
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
