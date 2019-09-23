package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "entity", propOrder = { "description", "accessMethods", "multitenant", "additionalCriteria", "customizer", "changeTracking", "table", "secondaryTable", "struct", "primaryKeyJoinColumn", "noSql", "cascadeOnDelete", "index", "idClass", "primaryKey", "inheritance", "discriminatorValue", "discriminatorColumn", "classExtractor", "optimisticLocking", "cache", "cacheInterceptor", "cacheIndex", "fetchGroup", "converter", "typeConverter", "objectTypeConverter", "structConverter", "copyPolicy", "instantiationCopyPolicy", "cloneCopyPolicy", "sequenceGenerator", "tableGenerator", "uuidGenerator", "partitioning", "replicationPartitioning", "roundRobinPartitioning", "pinnedPartitioning", "rangePartitioning", "valuePartitioning", "hashPartitioning", "unionPartitioning", "partitioned", "namedQuery", "namedNativeQuery", "namedStoredProcedureQuery", "namedStoredFunctionQuery", "namedPlsqlStoredProcedureQuery", "namedPlsqlStoredFunctionQuery", "plsqlRecord", "plsqlTable", "sqlResultSetMapping", "queryRedirectors", "excludeDefaultListeners", "excludeSuperclassListeners", "entityListeners", "prePersist", "postPersist", "preRemove", "postRemove", "preUpdate", "postUpdate", "postLoad", "property", "attributeOverride", "associationOverride", "attributes" })
public class Entity
{
    protected String description;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    protected Multitenant multitenant;
    @XmlElement(name = "additional-criteria")
    protected AdditionalCriteria additionalCriteria;
    protected Customizer customizer;
    @XmlElement(name = "change-tracking")
    protected ChangeTracking changeTracking;
    protected Table table;
    @XmlElement(name = "secondary-table")
    protected List<SecondaryTable> secondaryTable;
    protected Struct struct;
    @XmlElement(name = "primary-key-join-column")
    protected List<PrimaryKeyJoinColumn> primaryKeyJoinColumn;
    @XmlElement(name = "no-sql")
    protected NoSql noSql;
    @XmlElement(name = "cascade-on-delete")
    protected Boolean cascadeOnDelete;
    protected List<Index> index;
    @XmlElement(name = "id-class")
    protected IdClass idClass;
    @XmlElement(name = "primary-key")
    protected PrimaryKey primaryKey;
    protected Inheritance inheritance;
    @XmlElement(name = "discriminator-value")
    protected String discriminatorValue;
    @XmlElement(name = "discriminator-column")
    protected DiscriminatorColumn discriminatorColumn;
    @XmlElement(name = "class-extractor")
    protected ClassExtractor classExtractor;
    @XmlElement(name = "optimistic-locking")
    protected OptimisticLocking optimisticLocking;
    protected Cache cache;
    @XmlElement(name = "cache-interceptor")
    protected CacheInterceptor cacheInterceptor;
    @XmlElement(name = "cache-index")
    protected List<CacheIndex> cacheIndex;
    @XmlElement(name = "fetch-group")
    protected List<FetchGroup> fetchGroup;
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
    @XmlElement(name = "sequence-generator")
    protected SequenceGenerator sequenceGenerator;
    @XmlElement(name = "table-generator")
    protected TableGenerator tableGenerator;
    @XmlElement(name = "uuid-generator")
    protected UuidGenerator uuidGenerator;
    protected Partitioning partitioning;
    @XmlElement(name = "replication-partitioning")
    protected ReplicationPartitioning replicationPartitioning;
    @XmlElement(name = "round-robin-partitioning")
    protected RoundRobinPartitioning roundRobinPartitioning;
    @XmlElement(name = "pinned-partitioning")
    protected PinnedPartitioning pinnedPartitioning;
    @XmlElement(name = "range-partitioning")
    protected RangePartitioning rangePartitioning;
    @XmlElement(name = "value-partitioning")
    protected ValuePartitioning valuePartitioning;
    @XmlElement(name = "hash-partitioning")
    protected HashPartitioning hashPartitioning;
    @XmlElement(name = "union-partitioning")
    protected UnionPartitioning unionPartitioning;
    protected String partitioned;
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
    @XmlElement(name = "query-redirectors")
    protected QueryRedirectors queryRedirectors;
    @XmlElement(name = "exclude-default-listeners")
    protected EmptyType excludeDefaultListeners;
    @XmlElement(name = "exclude-superclass-listeners")
    protected EmptyType excludeSuperclassListeners;
    @XmlElement(name = "entity-listeners")
    protected EntityListeners entityListeners;
    @XmlElement(name = "pre-persist")
    protected PrePersist prePersist;
    @XmlElement(name = "post-persist")
    protected PostPersist postPersist;
    @XmlElement(name = "pre-remove")
    protected PreRemove preRemove;
    @XmlElement(name = "post-remove")
    protected PostRemove postRemove;
    @XmlElement(name = "pre-update")
    protected PreUpdate preUpdate;
    @XmlElement(name = "post-update")
    protected PostUpdate postUpdate;
    @XmlElement(name = "post-load")
    protected PostLoad postLoad;
    protected List<Property> property;
    @XmlElement(name = "attribute-override")
    protected List<AttributeOverride> attributeOverride;
    @XmlElement(name = "association-override")
    protected List<AssociationOverride> associationOverride;
    protected Attributes attributes;
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "class", required = true)
    protected String clazz;
    @XmlAttribute(name = "parent-class")
    protected String parentClass;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "cacheable")
    protected Boolean cacheable;
    @XmlAttribute(name = "metadata-complete")
    protected Boolean metadataComplete;
    @XmlAttribute(name = "read-only")
    protected Boolean readOnly;
    @XmlAttribute(name = "existence-checking")
    protected ExistenceType existenceChecking;
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
    
    public Multitenant getMultitenant() {
        return this.multitenant;
    }
    
    public void setMultitenant(final Multitenant value) {
        this.multitenant = value;
    }
    
    public AdditionalCriteria getAdditionalCriteria() {
        return this.additionalCriteria;
    }
    
    public void setAdditionalCriteria(final AdditionalCriteria value) {
        this.additionalCriteria = value;
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
    
    public Table getTable() {
        return this.table;
    }
    
    public void setTable(final Table value) {
        this.table = value;
    }
    
    public List<SecondaryTable> getSecondaryTable() {
        if (this.secondaryTable == null) {
            this.secondaryTable = new ArrayList<SecondaryTable>();
        }
        return this.secondaryTable;
    }
    
    public Struct getStruct() {
        return this.struct;
    }
    
    public void setStruct(final Struct value) {
        this.struct = value;
    }
    
    public List<PrimaryKeyJoinColumn> getPrimaryKeyJoinColumn() {
        if (this.primaryKeyJoinColumn == null) {
            this.primaryKeyJoinColumn = new ArrayList<PrimaryKeyJoinColumn>();
        }
        return this.primaryKeyJoinColumn;
    }
    
    public NoSql getNoSql() {
        return this.noSql;
    }
    
    public void setNoSql(final NoSql value) {
        this.noSql = value;
    }
    
    public Boolean isCascadeOnDelete() {
        return this.cascadeOnDelete;
    }
    
    public void setCascadeOnDelete(final Boolean value) {
        this.cascadeOnDelete = value;
    }
    
    public List<Index> getIndex() {
        if (this.index == null) {
            this.index = new ArrayList<Index>();
        }
        return this.index;
    }
    
    public IdClass getIdClass() {
        return this.idClass;
    }
    
    public void setIdClass(final IdClass value) {
        this.idClass = value;
    }
    
    public PrimaryKey getPrimaryKey() {
        return this.primaryKey;
    }
    
    public void setPrimaryKey(final PrimaryKey value) {
        this.primaryKey = value;
    }
    
    public Inheritance getInheritance() {
        return this.inheritance;
    }
    
    public void setInheritance(final Inheritance value) {
        this.inheritance = value;
    }
    
    public String getDiscriminatorValue() {
        return this.discriminatorValue;
    }
    
    public void setDiscriminatorValue(final String value) {
        this.discriminatorValue = value;
    }
    
    public DiscriminatorColumn getDiscriminatorColumn() {
        return this.discriminatorColumn;
    }
    
    public void setDiscriminatorColumn(final DiscriminatorColumn value) {
        this.discriminatorColumn = value;
    }
    
    public ClassExtractor getClassExtractor() {
        return this.classExtractor;
    }
    
    public void setClassExtractor(final ClassExtractor value) {
        this.classExtractor = value;
    }
    
    public OptimisticLocking getOptimisticLocking() {
        return this.optimisticLocking;
    }
    
    public void setOptimisticLocking(final OptimisticLocking value) {
        this.optimisticLocking = value;
    }
    
    public Cache getCache() {
        return this.cache;
    }
    
    public void setCache(final Cache value) {
        this.cache = value;
    }
    
    public CacheInterceptor getCacheInterceptor() {
        return this.cacheInterceptor;
    }
    
    public void setCacheInterceptor(final CacheInterceptor value) {
        this.cacheInterceptor = value;
    }
    
    public List<CacheIndex> getCacheIndex() {
        if (this.cacheIndex == null) {
            this.cacheIndex = new ArrayList<CacheIndex>();
        }
        return this.cacheIndex;
    }
    
    public List<FetchGroup> getFetchGroup() {
        if (this.fetchGroup == null) {
            this.fetchGroup = new ArrayList<FetchGroup>();
        }
        return this.fetchGroup;
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
    
    public SequenceGenerator getSequenceGenerator() {
        return this.sequenceGenerator;
    }
    
    public void setSequenceGenerator(final SequenceGenerator value) {
        this.sequenceGenerator = value;
    }
    
    public TableGenerator getTableGenerator() {
        return this.tableGenerator;
    }
    
    public void setTableGenerator(final TableGenerator value) {
        this.tableGenerator = value;
    }
    
    public UuidGenerator getUuidGenerator() {
        return this.uuidGenerator;
    }
    
    public void setUuidGenerator(final UuidGenerator value) {
        this.uuidGenerator = value;
    }
    
    public Partitioning getPartitioning() {
        return this.partitioning;
    }
    
    public void setPartitioning(final Partitioning value) {
        this.partitioning = value;
    }
    
    public ReplicationPartitioning getReplicationPartitioning() {
        return this.replicationPartitioning;
    }
    
    public void setReplicationPartitioning(final ReplicationPartitioning value) {
        this.replicationPartitioning = value;
    }
    
    public RoundRobinPartitioning getRoundRobinPartitioning() {
        return this.roundRobinPartitioning;
    }
    
    public void setRoundRobinPartitioning(final RoundRobinPartitioning value) {
        this.roundRobinPartitioning = value;
    }
    
    public PinnedPartitioning getPinnedPartitioning() {
        return this.pinnedPartitioning;
    }
    
    public void setPinnedPartitioning(final PinnedPartitioning value) {
        this.pinnedPartitioning = value;
    }
    
    public RangePartitioning getRangePartitioning() {
        return this.rangePartitioning;
    }
    
    public void setRangePartitioning(final RangePartitioning value) {
        this.rangePartitioning = value;
    }
    
    public ValuePartitioning getValuePartitioning() {
        return this.valuePartitioning;
    }
    
    public void setValuePartitioning(final ValuePartitioning value) {
        this.valuePartitioning = value;
    }
    
    public HashPartitioning getHashPartitioning() {
        return this.hashPartitioning;
    }
    
    public void setHashPartitioning(final HashPartitioning value) {
        this.hashPartitioning = value;
    }
    
    public UnionPartitioning getUnionPartitioning() {
        return this.unionPartitioning;
    }
    
    public void setUnionPartitioning(final UnionPartitioning value) {
        this.unionPartitioning = value;
    }
    
    public String getPartitioned() {
        return this.partitioned;
    }
    
    public void setPartitioned(final String value) {
        this.partitioned = value;
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
    
    public QueryRedirectors getQueryRedirectors() {
        return this.queryRedirectors;
    }
    
    public void setQueryRedirectors(final QueryRedirectors value) {
        this.queryRedirectors = value;
    }
    
    public EmptyType getExcludeDefaultListeners() {
        return this.excludeDefaultListeners;
    }
    
    public void setExcludeDefaultListeners(final EmptyType value) {
        this.excludeDefaultListeners = value;
    }
    
    public EmptyType getExcludeSuperclassListeners() {
        return this.excludeSuperclassListeners;
    }
    
    public void setExcludeSuperclassListeners(final EmptyType value) {
        this.excludeSuperclassListeners = value;
    }
    
    public EntityListeners getEntityListeners() {
        return this.entityListeners;
    }
    
    public void setEntityListeners(final EntityListeners value) {
        this.entityListeners = value;
    }
    
    public PrePersist getPrePersist() {
        return this.prePersist;
    }
    
    public void setPrePersist(final PrePersist value) {
        this.prePersist = value;
    }
    
    public PostPersist getPostPersist() {
        return this.postPersist;
    }
    
    public void setPostPersist(final PostPersist value) {
        this.postPersist = value;
    }
    
    public PreRemove getPreRemove() {
        return this.preRemove;
    }
    
    public void setPreRemove(final PreRemove value) {
        this.preRemove = value;
    }
    
    public PostRemove getPostRemove() {
        return this.postRemove;
    }
    
    public void setPostRemove(final PostRemove value) {
        this.postRemove = value;
    }
    
    public PreUpdate getPreUpdate() {
        return this.preUpdate;
    }
    
    public void setPreUpdate(final PreUpdate value) {
        this.preUpdate = value;
    }
    
    public PostUpdate getPostUpdate() {
        return this.postUpdate;
    }
    
    public void setPostUpdate(final PostUpdate value) {
        this.postUpdate = value;
    }
    
    public PostLoad getPostLoad() {
        return this.postLoad;
    }
    
    public void setPostLoad(final PostLoad value) {
        this.postLoad = value;
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
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
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
    
    public Boolean isCacheable() {
        return this.cacheable;
    }
    
    public void setCacheable(final Boolean value) {
        this.cacheable = value;
    }
    
    public Boolean isMetadataComplete() {
        return this.metadataComplete;
    }
    
    public void setMetadataComplete(final Boolean value) {
        this.metadataComplete = value;
    }
    
    public Boolean isReadOnly() {
        return this.readOnly;
    }
    
    public void setReadOnly(final Boolean value) {
        this.readOnly = value;
    }
    
    public ExistenceType getExistenceChecking() {
        return this.existenceChecking;
    }
    
    public void setExistenceChecking(final ExistenceType value) {
        this.existenceChecking = value;
    }
    
    public Boolean isExcludeDefaultMappings() {
        return this.excludeDefaultMappings;
    }
    
    public void setExcludeDefaultMappings(final Boolean value) {
        this.excludeDefaultMappings = value;
    }
}
