package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "element-collection", propOrder = { "orderBy", "orderColumn", "mapKey", "mapKeyClass", "mapKeyTemporal", "mapKeyEnumerated", "mapKeyConvert", "mapKeyAttributeOverride", "mapKeyAssociationOverride", "mapKeyColumn", "mapKeyJoinColumn", "column", "temporal", "enumerated", "lob", "convert", "attributeOverride", "associationOverride", "converterOrTypeConverterOrObjectTypeConverter", "collectionTable", "field", "cascadeOnDelete", "joinFetch", "batchFetch", "property", "accessMethods", "noncacheable", "deleteAll", "partitioning", "replicationPartitioning", "roundRobinPartitioning", "pinnedPartitioning", "rangePartitioning", "valuePartitioning", "hashPartitioning", "unionPartitioning", "partitioned" })
public class ElementCollection
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
    protected Column column;
    protected TemporalType temporal;
    protected EnumType enumerated;
    protected Lob lob;
    protected String convert;
    @XmlElement(name = "attribute-override")
    protected List<AttributeOverride> attributeOverride;
    @XmlElement(name = "association-override")
    protected List<AssociationOverride> associationOverride;
    @XmlElements({ @XmlElement(name = "converter", type = Converter.class), @XmlElement(name = "type-converter", type = TypeConverter.class), @XmlElement(name = "object-type-converter", type = ObjectTypeConverter.class), @XmlElement(name = "struct-converter", type = StructConverter.class) })
    protected List<Object> converterOrTypeConverterOrObjectTypeConverter;
    @XmlElement(name = "collection-table")
    protected CollectionTable collectionTable;
    protected Field field;
    @XmlElement(name = "cascade-on-delete")
    protected Boolean cascadeOnDelete;
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
    @XmlAttribute(name = "target-class")
    protected String targetClass;
    @XmlAttribute(name = "fetch")
    protected FetchType fetch;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "attribute-type")
    protected String attributeType;
    @XmlAttribute(name = "composite-member")
    protected String compositeMember;
    
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
    
    public List<Object> getConverterOrTypeConverterOrObjectTypeConverter() {
        if (this.converterOrTypeConverterOrObjectTypeConverter == null) {
            this.converterOrTypeConverterOrObjectTypeConverter = new ArrayList<Object>();
        }
        return this.converterOrTypeConverterOrObjectTypeConverter;
    }
    
    public CollectionTable getCollectionTable() {
        return this.collectionTable;
    }
    
    public void setCollectionTable(final CollectionTable value) {
        this.collectionTable = value;
    }
    
    public Field getField() {
        return this.field;
    }
    
    public void setField(final Field value) {
        this.field = value;
    }
    
    public Boolean isCascadeOnDelete() {
        return this.cascadeOnDelete;
    }
    
    public void setCascadeOnDelete(final Boolean value) {
        this.cascadeOnDelete = value;
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
    
    public String getTargetClass() {
        return this.targetClass;
    }
    
    public void setTargetClass(final String value) {
        this.targetClass = value;
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
    
    public String getAttributeType() {
        return this.attributeType;
    }
    
    public void setAttributeType(final String value) {
        this.attributeType = value;
    }
    
    public String getCompositeMember() {
        return this.compositeMember;
    }
    
    public void setCompositeMember(final String value) {
        this.compositeMember = value;
    }
}
