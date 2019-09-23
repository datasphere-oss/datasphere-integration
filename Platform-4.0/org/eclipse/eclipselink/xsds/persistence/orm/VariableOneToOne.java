package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "variable-one-to-one", propOrder = { "cascade", "discriminatorColumn", "discriminatorClass", "joinColumn", "privateOwned", "property", "accessMethods", "noncacheable", "partitioning", "replicationPartitioning", "roundRobinPartitioning", "pinnedPartitioning", "rangePartitioning", "valuePartitioning", "hashPartitioning", "unionPartitioning", "partitioned" })
public class VariableOneToOne
{
    protected CascadeType cascade;
    @XmlElement(name = "discriminator-column")
    protected DiscriminatorColumn discriminatorColumn;
    @XmlElement(name = "discriminator-class")
    protected List<DiscriminatorClass> discriminatorClass;
    @XmlElement(name = "join-column")
    protected List<JoinColumn> joinColumn;
    @XmlElement(name = "private-owned")
    protected EmptyType privateOwned;
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
    @XmlAttribute(name = "target-interface")
    protected String targetInterface;
    @XmlAttribute(name = "fetch")
    protected FetchType fetch;
    @XmlAttribute(name = "optional")
    protected Boolean optional;
    @XmlAttribute(name = "access")
    protected AccessType access;
    @XmlAttribute(name = "orphan-removal")
    protected Boolean orphanRemoval;
    
    public CascadeType getCascade() {
        return this.cascade;
    }
    
    public void setCascade(final CascadeType value) {
        this.cascade = value;
    }
    
    public DiscriminatorColumn getDiscriminatorColumn() {
        return this.discriminatorColumn;
    }
    
    public void setDiscriminatorColumn(final DiscriminatorColumn value) {
        this.discriminatorColumn = value;
    }
    
    public List<DiscriminatorClass> getDiscriminatorClass() {
        if (this.discriminatorClass == null) {
            this.discriminatorClass = new ArrayList<DiscriminatorClass>();
        }
        return this.discriminatorClass;
    }
    
    public List<JoinColumn> getJoinColumn() {
        if (this.joinColumn == null) {
            this.joinColumn = new ArrayList<JoinColumn>();
        }
        return this.joinColumn;
    }
    
    public EmptyType getPrivateOwned() {
        return this.privateOwned;
    }
    
    public void setPrivateOwned(final EmptyType value) {
        this.privateOwned = value;
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
    
    public String getTargetInterface() {
        return this.targetInterface;
    }
    
    public void setTargetInterface(final String value) {
        this.targetInterface = value;
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
    
    public Boolean isOrphanRemoval() {
        return this.orphanRemoval;
    }
    
    public void setOrphanRemoval(final Boolean value) {
        this.orphanRemoval = value;
    }
}
