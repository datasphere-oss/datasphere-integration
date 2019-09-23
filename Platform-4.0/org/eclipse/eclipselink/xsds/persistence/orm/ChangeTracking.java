package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "change-tracking")
public class ChangeTracking
{
    @XmlAttribute(name = "type")
    protected ChangeTrackingType type;
    
    public ChangeTrackingType getType() {
        return this.type;
    }
    
    public void setType(final ChangeTrackingType value) {
        this.type = value;
    }
}
