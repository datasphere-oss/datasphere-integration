package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "return-insert")
public class ReturnInsert
{
    @XmlAttribute(name = "return-only")
    protected Boolean returnOnly;
    
    public Boolean isReturnOnly() {
        return this.returnOnly;
    }
    
    public void setReturnOnly(final Boolean value) {
        this.returnOnly = value;
    }
}
