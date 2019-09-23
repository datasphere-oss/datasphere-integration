package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "join-field")
public class JoinField
{
    @XmlAttribute(name = "name")
    protected String name;
    @XmlAttribute(name = "referenced-field-name")
    protected String referencedFieldName;
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public String getReferencedFieldName() {
        return this.referencedFieldName;
    }
    
    public void setReferencedFieldName(final String value) {
        this.referencedFieldName = value;
    }
}
