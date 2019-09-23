package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "one-to-many", propOrder = { "orderBy", "orderColumn", "mapKey", "mapKeyClass", "mapKeyTemporal", "mapKeyEnumerated", "mapKeyConvert", "mapKeyAttributeOverride", "mapKeyAssociationOverride", "mapKeyColumn", "mapKeyJoinColumn", "converter", "typeConverter", "objectTypeConverter", "structConverter", "joinTable", "joinColumn", "joinField", "cascade", "cascadeOnDelete", "privateOwned", "joinFetch", "batchFetch", "property", "accessMethods", "noncacheable", "deleteAll", "partitioning", "replicationPartitioning", "roundRobinPartitioning", "pinnedPartitioning", "rangePartitioning", "valuePartitioning", "hashPartitioning", "unionPartitioning", "partitioned" })
public class OneToMany
{
    @XmlElement(name = "order-by")
    protected String orderBy;
    @XmlElement(name = "order-column")
    protected OrderColumn orderColumn;
    @XmlElement(name = "map-key")
    protected MapKey mapKey;
    @XmlElement(name = "map-key-class")
    protected MapKeyClass mapKeyClass;
    @XmlElement(name = "map-key-temporal")
    protected TemporalType mapKeyTemporal;
    @XmlElement(name = "map-key-enumerated")
    protected EnumType mapKeyEnumerated;
    @XmlElement(name = "map-key-convert")
    protected String mapKeyConvert;
    @XmlElement(name = "map-key-attribute-override")
    protected List<AttributeOverride> mapKeyAttributeOverride;
    @XmlElement(name = "map-key-association-override")
    protected List<AssociationOverride> mapKeyAssociationOverride;
    @XmlElement(name = "map-key-column")
    protected MapKeyColumn mapKeyColumn;
    @XmlElement(name = "map-key-join-column")
    protected List<MapKeyJoinColumn> mapKeyJoinColumn;
    protected Converter converter;
    @XmlElement(name = "type-converter")
    protected TypeConverter typeConverter;
    @XmlElement(name = "object-type-converter")
    protected ObjectTypeConverter objectTypeConverter;
    @XmlElement(name = "struct-converter")
    protected StructConverter structConverter;
    @XmlElement(name = "join-table")
    protected JoinTable joinTable;
    @XmlElement(name = "join-column")
    protected List<JoinColumn> joinColumn;
    @XmlElement(name = "join-field")
    protected List<JoinField> joinField;
    protected CascadeType cascade;
    @XmlElement(name = "cascade-on-delete")
    protected Boolean cascadeOnDelete;
    @XmlElement(name = "private-owned")
    protected EmptyType privateOwned;
    @XmlElement(name = "join-fetch")
    protected JoinFetchType joinFetch;
    @XmlElement(name = "batch-fetch")
    protected BatchFetch batchFetch;
    protected List<Property> property;
    @XmlElement(name = "access-methods")
    protected AccessMethods accessMethods;
    protected EmptyType noncacheable;
    @XmlElement(name = "delete-all")
    protected EmptyType deleteAll;
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
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "target-entity")
    protected String targetEntity;
    @XmlAttribute(name = "fetch")
    protected FetchType fetch;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "mapped-by")
    protected String mappedBy;
    @XmlAttribute(name = "orphan-removal")
    protected Boolean orphanRemoval;
    @XmlAttribute(name = "attribute-type")
    protected String attributeType;
    
    public String getOrderBy() {
        return this.orderBy;
    }
    
    public void setOrderBy(final String value) {
        this.orderBy = value;
    }
    
    public OrderColumn getOrderColumn() {
        return this.orderColumn;
    }
    
    public void setOrderColumn(final OrderColumn value) {
        this.orderColumn = value;
    }
    
    public MapKey getMapKey() {
        return this.mapKey;
    }
    
    public void setMapKey(final MapKey value) {
        this.mapKey = value;
    }
    
    public MapKeyClass getMapKeyClass() {
        return this.mapKeyClass;
    }
    
    public void setMapKeyClass(final MapKeyClass value) {
        this.mapKeyClass = value;
    }
    
    public TemporalType getMapKeyTemporal() {
        return this.mapKeyTemporal;
    }
    
    public void setMapKeyTemporal(final TemporalType value) {
        this.mapKeyTemporal = value;
    }
    
    public EnumType getMapKeyEnumerated() {
        return this.mapKeyEnumerated;
    }
    
    public void setMapKeyEnumerated(final EnumType value) {
        this.mapKeyEnumerated = value;
    }
    
    public String getMapKeyConvert() {
        return this.mapKeyConvert;
    }
    
    public void setMapKeyConvert(final String value) {
        this.mapKeyConvert = value;
    }
    
    public List<AttributeOverride> getMapKeyAttributeOverride() {
        if (this.mapKeyAttributeOverride == null) {
            this.mapKeyAttributeOverride = new ArrayList<AttributeOverride>();
        }
        return this.mapKeyAttributeOverride;
    }
    
    public List<AssociationOverride> getMapKeyAssociationOverride() {
        if (this.mapKeyAssociationOverride == null) {
            this.mapKeyAssociationOverride = new ArrayList<AssociationOverride>();
        }
        return this.mapKeyAssociationOverride;
    }
    
    public MapKeyColumn getMapKeyColumn() {
        return this.mapKeyColumn;
    }
    
    public void setMapKeyColumn(final MapKeyColumn value) {
        this.mapKeyColumn = value;
    }
    
    public List<MapKeyJoinColumn> getMapKeyJoinColumn() {
        if (this.mapKeyJoinColumn == null) {
            this.mapKeyJoinColumn = new ArrayList<MapKeyJoinColumn>();
        }
        return this.mapKeyJoinColumn;
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
    
    public JoinTable getJoinTable() {
        return this.joinTable;
    }
    
    public void setJoinTable(final JoinTable value) {
        this.joinTable = value;
    }
    
    public List<JoinColumn> getJoinColumn() {
        if (this.joinColumn == null) {
            this.joinColumn = new ArrayList<JoinColumn>();
        }
        return this.joinColumn;
    }
    
    public List<JoinField> getJoinField() {
        if (this.joinField == null) {
            this.joinField = new ArrayList<JoinField>();
        }
        return this.joinField;
    }
    
    public CascadeType getCascade() {
        return this.cascade;
    }
    
    public void setCascade(final CascadeType value) {
        this.cascade = value;
    }
    
    public Boolean isCascadeOnDelete() {
        return this.cascadeOnDelete;
    }
    
    public void setCascadeOnDelete(final Boolean value) {
        this.cascadeOnDelete = value;
    }
    
    public EmptyType getPrivateOwned() {
        return this.privateOwned;
    }
    
    public void setPrivateOwned(final EmptyType value) {
        this.privateOwned = value;
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
    
    public EmptyType getDeleteAll() {
        return this.deleteAll;
    }
    
    public void setDeleteAll(final EmptyType value) {
        this.deleteAll = value;
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
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getTargetEntity() {
        return this.targetEntity;
    }
    
    public void setTargetEntity(final String value) {
        this.targetEntity = value;
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
    
    public String getMappedBy() {
        return this.mappedBy;
    }
    
    public void setMappedBy(final String value) {
        this.mappedBy = value;
    }
    
    public Boolean isOrphanRemoval() {
        return this.orphanRemoval;
    }
    
    public void setOrphanRemoval(final Boolean value) {
        this.orphanRemoval = value;
    }
    
    public String getAttributeType() {
        return this.attributeType;
    }
    
    public void setAttributeType(final String value) {
        this.attributeType = value;
    }
}
