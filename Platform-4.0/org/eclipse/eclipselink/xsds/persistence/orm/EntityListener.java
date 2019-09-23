package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "entity-listener", propOrder = { "description", "prePersist", "postPersist", "preRemove", "postRemove", "preUpdate", "postUpdate", "postLoad" })
public class EntityListener
{
    protected String description;
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
    @XmlAttribute(name = "class", required = true)
    protected String clazz;
    
    public String getDescription() {
        return this.description;
    }
    
    public void setDescription(final String value) {
        this.description = value;
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
    
    public String getClazz() {
        return this.clazz;
    }
    
    public void setClazz(final String value) {
        this.clazz = value;
    }
}
