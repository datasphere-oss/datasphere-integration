package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = { "description", "persistenceUnitMetadata", "_package", "schema", "catalog", "access", "accessMethods", "tenantDiscriminatorColumn", "converter", "typeConverter", "objectTypeConverter", "structConverter", "sequenceGenerator", "tableGenerator", "uuidGenerator", "partitioning", "replicationPartitioning", "roundRobinPartitioning", "pinnedPartitioning", "rangePartitioning", "valuePartitioning", "hashPartitioning", "unionPartitioning", "namedQuery", "namedNativeQuery", "namedStoredProcedureQuery", "namedStoredFunctionQuery", "namedPlsqlStoredProcedureQuery", "namedPlsqlStoredFunctionQuery", "plsqlRecord", "plsqlTable", "sqlResultSetMapping", "mappedSuperclass", "entity", "embeddable" })
@XmlRootElement(name = "entity-mappings")
public class EntityMappings
{
    protected String description;
    @XmlElement(name = "persistence-unit-metadata")
    protected PersistenceUnitMetadata persistenceUnitMetadata;
    @XmlElement(name = "package")
    protected String _package;
    protected String schema;
    protected String catalog;
    protected AccessType access;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    @XmlElement(name = "tenant-discriminator-column")
    protected List<TenantDiscriminatorColumn> tenantDiscriminatorColumn;
    protected List<Converter> converter;
    @XmlElement(name = "type-converter")
    protected List<TypeConverter> typeConverter;
    @XmlElement(name = "object-type-converter")
    protected List<ObjectTypeConverter> objectTypeConverter;
    @XmlElement(name = "struct-converter")
    protected List<StructConverter> structConverter;
    @XmlElement(name = "sequence-generator")
    protected List<SequenceGenerator> sequenceGenerator;
    @XmlElement(name = "table-generator")
    protected List<TableGenerator> tableGenerator;
    @XmlElement(name = "uuid-generator")
    protected List<UuidGenerator> uuidGenerator;
    protected List<Partitioning> partitioning;
    @XmlElement(name = "replication-partitioning")
    protected List<ReplicationPartitioning> replicationPartitioning;
    @XmlElement(name = "round-robin-partitioning")
    protected List<RoundRobinPartitioning> roundRobinPartitioning;
    @XmlElement(name = "pinned-partitioning")
    protected List<PinnedPartitioning> pinnedPartitioning;
    @XmlElement(name = "range-partitioning")
    protected List<RangePartitioning> rangePartitioning;
    @XmlElement(name = "value-partitioning")
    protected List<ValuePartitioning> valuePartitioning;
    @XmlElement(name = "hash-partitioning")
    protected List<HashPartitioning> hashPartitioning;
    @XmlElement(name = "union-partitioning")
    protected List<UnionPartitioning> unionPartitioning;
    @XmlElement(name = "named-query")
    protected List<NamedQuery> namedQuery;
    @XmlElement(name = "named-native-query")
    protected List<NamedNativeQuery> namedNativeQuery;
    @XmlElement(name = "named-stored-procedure-query")
    protected List<NamedStoredProcedureQuery> namedStoredProcedureQuery;
    @XmlElement(name = "named-stored-function-query")
    protected List<NamedStoredFunctionQuery> namedStoredFunctionQuery;
    @XmlElement(name = "named-plsql-stored-procedure-query")
    protected List<NamedPlsqlStoredProcedureQuery> namedPlsqlStoredProcedureQuery;
    @XmlElement(name = "named-plsql-stored-function-query")
    protected List<NamedPlsqlStoredFunctionQuery> namedPlsqlStoredFunctionQuery;
    @XmlElement(name = "plsql-record")
    protected List<PlsqlRecord> plsqlRecord;
    @XmlElement(name = "plsql-table")
    protected List<PlsqlTable> plsqlTable;
    @XmlElement(name = "sql-result-set-mapping")
    protected List<SqlResultSetMapping> sqlResultSetMapping;
    @XmlElement(name = "mapped-superclass")
    protected List<MappedSuperclass> mappedSuperclass;
    protected List<Entity> entity;
    protected List<Embeddable> embeddable;
    @XmlAttribute(name = "version")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    protected String version;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
    }
    
    public PersistenceUnitMetadata getPersistenceUnitMetadata() {
        return this.persistenceUnitMetadata;
    }
    
    public void setPersistenceUnitMetadata(final PersistenceUnitMetadata value) {
        this.persistenceUnitMetadata = value;
    }
    
    public String getPackage() {
        return this._package;
    }
    
    public void setPackage(final String value) {
        this._package = value;
    }
    
    public String getSchema() {
        return this.schema;
    }
    
    public void setSchema(final String value) {
        this.schema = value;
    }
    
    public String getCatalog() {
        return this.catalog;
    }
    
    public void setCatalog(final String value) {
        this.catalog = value;
    }
    
    public AccessType getAccess() {
        return this.access;
    }
    
    public void setAccess(final AccessType value) {
        this.access = value;
    }
    
    public AccessMethods getAccessMethods() {
        return this.accessMethods;
    }
    
    public void setAccessMethods(final AccessMethods value) {
        this.accessMethods = value;
    }
    
    public List<TenantDiscriminatorColumn> getTenantDiscriminatorColumn() {
        if (this.tenantDiscriminatorColumn == null) {
            this.tenantDiscriminatorColumn = new ArrayList<TenantDiscriminatorColumn>();
        }
        return this.tenantDiscriminatorColumn;
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
    
    public List<SequenceGenerator> getSequenceGenerator() {
        if (this.sequenceGenerator == null) {
            this.sequenceGenerator = new ArrayList<SequenceGenerator>();
        }
        return this.sequenceGenerator;
    }
    
    public List<TableGenerator> getTableGenerator() {
        if (this.tableGenerator == null) {
            this.tableGenerator = new ArrayList<TableGenerator>();
        }
        return this.tableGenerator;
    }
    
    public List<UuidGenerator> getUuidGenerator() {
        if (this.uuidGenerator == null) {
            this.uuidGenerator = new ArrayList<UuidGenerator>();
        }
        return this.uuidGenerator;
    }
    
    public List<Partitioning> getPartitioning() {
        if (this.partitioning == null) {
            this.partitioning = new ArrayList<Partitioning>();
        }
        return this.partitioning;
    }
    
    public List<ReplicationPartitioning> getReplicationPartitioning() {
        if (this.replicationPartitioning == null) {
            this.replicationPartitioning = new ArrayList<ReplicationPartitioning>();
        }
        return this.replicationPartitioning;
    }
    
    public List<RoundRobinPartitioning> getRoundRobinPartitioning() {
        if (this.roundRobinPartitioning == null) {
            this.roundRobinPartitioning = new ArrayList<RoundRobinPartitioning>();
        }
        return this.roundRobinPartitioning;
    }
    
    public List<PinnedPartitioning> getPinnedPartitioning() {
        if (this.pinnedPartitioning == null) {
            this.pinnedPartitioning = new ArrayList<PinnedPartitioning>();
        }
        return this.pinnedPartitioning;
    }
    
    public List<RangePartitioning> getRangePartitioning() {
        if (this.rangePartitioning == null) {
            this.rangePartitioning = new ArrayList<RangePartitioning>();
        }
        return this.rangePartitioning;
    }
    
    public List<ValuePartitioning> getValuePartitioning() {
        if (this.valuePartitioning == null) {
            this.valuePartitioning = new ArrayList<ValuePartitioning>();
        }
        return this.valuePartitioning;
    }
    
    public List<HashPartitioning> getHashPartitioning() {
        if (this.hashPartitioning == null) {
            this.hashPartitioning = new ArrayList<HashPartitioning>();
        }
        return this.hashPartitioning;
    }
    
    public List<UnionPartitioning> getUnionPartitioning() {
        if (this.unionPartitioning == null) {
            this.unionPartitioning = new ArrayList<UnionPartitioning>();
        }
        return this.unionPartitioning;
    }
    
    public List<NamedQuery> getNamedQuery() {
        if (this.namedQuery == null) {
            this.namedQuery = new ArrayList<NamedQuery>();
        }
        return this.namedQuery;
    }
    
    public List<NamedNativeQuery> getNamedNativeQuery() {
        if (this.namedNativeQuery == null) {
            this.namedNativeQuery = new ArrayList<NamedNativeQuery>();
        }
        return this.namedNativeQuery;
    }
    
    public List<NamedStoredProcedureQuery> getNamedStoredProcedureQuery() {
        if (this.namedStoredProcedureQuery == null) {
            this.namedStoredProcedureQuery = new ArrayList<NamedStoredProcedureQuery>();
        }
        return this.namedStoredProcedureQuery;
    }
    
    public List<NamedStoredFunctionQuery> getNamedStoredFunctionQuery() {
        if (this.namedStoredFunctionQuery == null) {
            this.namedStoredFunctionQuery = new ArrayList<NamedStoredFunctionQuery>();
        }
        return this.namedStoredFunctionQuery;
    }
    
    public List<NamedPlsqlStoredProcedureQuery> getNamedPlsqlStoredProcedureQuery() {
        if (this.namedPlsqlStoredProcedureQuery == null) {
            this.namedPlsqlStoredProcedureQuery = new ArrayList<NamedPlsqlStoredProcedureQuery>();
        }
        return this.namedPlsqlStoredProcedureQuery;
    }
    
    public List<NamedPlsqlStoredFunctionQuery> getNamedPlsqlStoredFunctionQuery() {
        if (this.namedPlsqlStoredFunctionQuery == null) {
            this.namedPlsqlStoredFunctionQuery = new ArrayList<NamedPlsqlStoredFunctionQuery>();
        }
        return this.namedPlsqlStoredFunctionQuery;
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
    
    public List<SqlResultSetMapping> getSqlResultSetMapping() {
        if (this.sqlResultSetMapping == null) {
            this.sqlResultSetMapping = new ArrayList<SqlResultSetMapping>();
        }
        return this.sqlResultSetMapping;
    }
    
    public List<MappedSuperclass> getMappedSuperclass() {
        if (this.mappedSuperclass == null) {
            this.mappedSuperclass = new ArrayList<MappedSuperclass>();
        }
        return this.mappedSuperclass;
    }
    
    public List<Entity> getEntity() {
        if (this.entity == null) {
            this.entity = new ArrayList<Entity>();
        }
        return this.entity;
    }
    
    public List<Embeddable> getEmbeddable() {
        if (this.embeddable == null) {
            this.embeddable = new ArrayList<Embeddable>();
        }
        return this.embeddable;
    }
    
    public String getVersion() {
        if (this.version == null) {
            return "2.3";
        }
        return this.version;
    }
    
    public void setVersion(final String value) {
        this.version = value;
    }
}
