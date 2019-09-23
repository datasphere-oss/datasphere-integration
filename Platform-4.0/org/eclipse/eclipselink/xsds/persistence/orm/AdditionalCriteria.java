package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "additional-criteria", propOrder = { "criteria" })
public class AdditionalCriteria
{
    @XmlElement(required = true)
    protected String criteria;
    
    public String getCriteria() {
        return this.criteria;
    }
    
    public void setCriteria(final String value) {
        this.criteria = value;
    }
}
