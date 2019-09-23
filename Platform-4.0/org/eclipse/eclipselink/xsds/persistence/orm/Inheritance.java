package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "inheritance")
public class Inheritance
{
    @XmlAttribute(name = "strategy")
    protected InheritanceType strategy;
    
    public InheritanceType getStrategy() {
        return this.strategy;
    }
    
    public void setStrategy(final InheritanceType value) {
        this.strategy = value;
    }
}
