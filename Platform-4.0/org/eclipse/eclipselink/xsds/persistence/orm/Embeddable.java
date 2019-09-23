package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "embeddable", propOrder = { "description", "accessMethods", "customizer", "changeTracking", "struct", "noSql", "converter", "typeConverter", "objectTypeConverter", "structConverter", "copyPolicy", "instantiationCopyPolicy", "cloneCopyPolicy", "plsqlRecord", "plsqlTable", "property", "attributeOverride", "associationOverride", "attributes" })
public class Embeddable
{
    protected String description;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    protected Customizer customizer;
    @XmlElement(name = "change-tracking")
    protected ChangeTracking changeTracking;
    protected Struct struct;
    @XmlElement(name = "no-sql")
    protected NoSql noSql;
    protected List<Converter> converter;
    @XmlElement(name = "type-converter")
    protected List<TypeConverter> typeConverter;
    @XmlElement(name = "object-type-converter")
    protected List<ObjectTypeConverter> objectTypeConverter;
    @XmlElement(name = "struct-converter")
    protected List<StructConverter> structConverter;
    @XmlElement(name = "copy-policy")
    protected CopyPolicy copyPolicy;
    @XmlElement(name = "instantiation-copy-policy")
    protected InstantiationCopyPolicy instantiationCopyPolicy;
    @XmlElement(name = "clone-copy-policy")
    protected CloneCopyPolicy cloneCopyPolicy;
    @XmlElement(name = "plsql-record")
    protected List<PlsqlRecord> plsqlRecord;
    @XmlElement(name = "plsql-table")
    protected List<PlsqlTable> plsqlTable;
    protected List<Property> property;
    @XmlElement(name = "attribute-override")
    protected List<AttributeOverride> attributeOverride;
    @XmlElement(name = "association-override")
    protected List<AssociationOverride> associationOverride;
    protected Attributes attributes;
    @XmlAttribute(name = "class", required = true)
    protected String clazz;
    @XmlAttribute(name = "parent-class")
    protected String parentClass;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "metadata-complete")
    protected Boolean metadataComplete;
    @XmlAttribute(name = "exclude-default-mappings")
    protected Boolean excludeDefaultMappings;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public AccessMethods getAccessMethods() {
        return this.accessMethods;
    }
    
    public void setAccessMethods(final AccessMethods value) {
        this.accessMethods = value;
    }
    
    public Customizer getCustomizer() {
        return this.customizer;
    }
    
    public void setCustomizer(final Customizer value) {
        this.customizer = value;
    }
    
    public ChangeTracking getChangeTracking() {
        return this.changeTracking;
    }
    
    public void setChangeTracking(final ChangeTracking value) {
        this.changeTracking = value;
    }
    
    public Struct getStruct() {
        return this.struct;
    }
    
    public void setStruct(final Struct value) {
        this.struct = value;
    }
    
    public NoSql getNoSql() {
        return this.noSql;
    }
    
    public void setNoSql(final NoSql value) {
        this.noSql = value;
    }
    
    public List<Converter> getConverter() {
        if (this.converter == null) {
            this.converter = new ArrayList<Converter>();
        }
        return this.converter;
    }
    
    public List<TypeConverter> getTypeConverter() {
        if (this.typeConverter == null) {
            this.typeConverter = new ArrayList<TypeConverter>();
        }
        return this.typeConverter;
    }
    
    public List<ObjectTypeConverter> getObjectTypeConverter() {
        if (this.objectTypeConverter == null) {
            this.objectTypeConverter = new ArrayList<ObjectTypeConverter>();
        }
        return this.objectTypeConverter;
    }
    
    public List<StructConverter> getStructConverter() {
        if (this.structConverter == null) {
            this.structConverter = new ArrayList<StructConverter>();
        }
        return this.structConverter;
    }
    
    public CopyPolicy getCopyPolicy() {
        return this.copyPolicy;
    }
    
    public void setCopyPolicy(final CopyPolicy value) {
        this.copyPolicy = value;
    }
    
    public InstantiationCopyPolicy getInstantiationCopyPolicy() {
        return this.instantiationCopyPolicy;
    }
    
    public void setInstantiationCopyPolicy(final InstantiationCopyPolicy value) {
        this.instantiationCopyPolicy = value;
    }
    
    public CloneCopyPolicy getCloneCopyPolicy() {
        return this.cloneCopyPolicy;
    }
    
    public void setCloneCopyPolicy(final CloneCopyPolicy value) {
        this.cloneCopyPolicy = value;
    }
    
    public List<PlsqlRecord> getPlsqlRecord() {
        if (this.plsqlRecord == null) {
            this.plsqlRecord = new ArrayList<PlsqlRecord>();
        }
        return this.plsqlRecord;
    }
    
    public List<PlsqlTable> getPlsqlTable() {
        if (this.plsqlTable == null) {
            this.plsqlTable = new ArrayList<PlsqlTable>();
        }
        return this.plsqlTable;
    }
    
    public List<Property> getProperty() {
        if (this.property == null) {
            this.property = new ArrayList<Property>();
        }
        return this.property;
    }
    
    public List<AttributeOverride> getAttributeOverride() {
        if (this.attributeOverride == null) {
            this.attributeOverride = new ArrayList<AttributeOverride>();
        }
        return this.attributeOverride;
    }
    
    public List<AssociationOverride> getAssociationOverride() {
        if (this.associationOverride == null) {
            this.associationOverride = new ArrayList<AssociationOverride>();
        }
        return this.associationOverride;
    }
    
    public Attributes getAttributes() {
        return this.attributes;
    }
    
    public void setAttributes(final Attributes value) {
        this.attributes = value;
    }
    
    public String getClazz() {
        return this.clazz;
    }
    
    public void setClazz(final String value) {
        this.clazz = value;
    }
    
    public String getParentClass() {
        return this.parentClass;
    }
    
    public void setParentClass(final String value) {
        this.parentClass = value;
    }
    
    public AccessType getAccess() {
        return this.access;
    }
    
    public void setAccess(final AccessType value) {
        this.access = value;
    }
    
    public Boolean isMetadataComplete() {
        return this.metadataComplete;
    }
    
    public void setMetadataComplete(final Boolean value) {
        this.metadataComplete = value;
    }
    
    public Boolean isExcludeDefaultMappings() {
        return this.excludeDefaultMappings;
    }
    
    public void setExcludeDefaultMappings(final Boolean value) {
        this.excludeDefaultMappings = value;
    }
}
