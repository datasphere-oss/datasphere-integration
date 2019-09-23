package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "cascade-type", propOrder = { "cascadeAll", "cascadePersist", "cascadeMerge", "cascadeRemove", "cascadeRefresh", "cascadeDetach" })
public class CascadeType
{
    @XmlElement(name = "cascade-all")
    protected EmptyType cascadeAll;
    @XmlElement(name = "cascade-persist")
    protected EmptyType cascadePersist;
    @XmlElement(name = "cascade-merge")
    protected EmptyType cascadeMerge;
    @XmlElement(name = "cascade-remove")
    protected EmptyType cascadeRemove;
    @XmlElement(name = "cascade-refresh")
    protected EmptyType cascadeRefresh;
    @XmlElement(name = "cascade-detach")
    protected EmptyType cascadeDetach;
    
    public EmptyType getCascadeAll() {
        return this.cascadeAll;
    }
    
    public void setCascadeAll(final EmptyType value) {
        this.cascadeAll = value;
    }
    
    public EmptyType getCascadePersist() {
        return this.cascadePersist;
    }
    
    public void setCascadePersist(final EmptyType value) {
        this.cascadePersist = value;
    }
    
    public EmptyType getCascadeMerge() {
        return this.cascadeMerge;
    }
    
    public void setCascadeMerge(final EmptyType value) {
        this.cascadeMerge = value;
    }
    
    public EmptyType getCascadeRemove() {
        return this.cascadeRemove;
    }
    
    public void setCascadeRemove(final EmptyType value) {
        this.cascadeRemove = value;
    }
    
    public EmptyType getCascadeRefresh() {
        return this.cascadeRefresh;
    }
    
    public void setCascadeRefresh(final EmptyType value) {
        this.cascadeRefresh = value;
    }
    
    public EmptyType getCascadeDetach() {
        return this.cascadeDetach;
    }
    
    public void setCascadeDetach(final EmptyType value) {
        this.cascadeDetach = value;
    }
}
