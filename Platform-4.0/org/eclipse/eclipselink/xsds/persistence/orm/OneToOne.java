package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "one-to-one", propOrder = { "primaryKeyJoinColumn", "joinColumn", "joinTable", "joinField", "cascade", "cascadeOnDelete", "privateOwned", "joinFetch", "batchFetch", "property", "accessMethods", "noncacheable", "partitioning", "replicationPartitioning", "roundRobinPartitioning", "pinnedPartitioning", "rangePartitioning", "valuePartitioning", "hashPartitioning", "unionPartitioning", "partitioned" })
public class OneToOne
{
    @XmlElement(name = "primary-key-join-column")
    protected List<PrimaryKeyJoinColumn> primaryKeyJoinColumn;
    @XmlElement(name = "join-column")
    protected List<JoinColumn> joinColumn;
    @XmlElement(name = "join-table")
    protected JoinTable joinTable;
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
    @XmlAttribute(name = "optional")
    protected Boolean optional;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "mapped-by")
    protected String mappedBy;
    @XmlAttribute(name = "orphan-removal")
    protected Boolean orphanRemoval;
    @XmlAttribute(name = "maps-id")
    protected String mapsId;
    @XmlAttribute(name = "id")
    protected Boolean id;
    
    public List<PrimaryKeyJoinColumn> getPrimaryKeyJoinColumn() {
        if (this.primaryKeyJoinColumn == null) {
            this.primaryKeyJoinColumn = new ArrayList<PrimaryKeyJoinColumn>();
        }
        return this.primaryKeyJoinColumn;
    }
    
    public List<JoinColumn> getJoinColumn() {
        if (this.joinColumn == null) {
            this.joinColumn = new ArrayList<JoinColumn>();
        }
        return this.joinColumn;
    }
    
    public JoinTable getJoinTable() {
        return this.joinTable;
    }
    
    public void setJoinTable(final JoinTable value) {
        this.joinTable = value;
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
    
    public String getMapsId() {
        return this.mapsId;
    }
    
    public void setMapsId(final String value) {
        this.mapsId = value;
    }
    
    public Boolean isId() {
        return this.id;
    }
    
    public void setId(final Boolean value) {
        this.id = value;
    }
}
