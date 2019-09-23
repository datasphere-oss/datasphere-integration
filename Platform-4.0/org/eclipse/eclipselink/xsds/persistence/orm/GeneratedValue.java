package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "generated-value")
public class GeneratedValue
{
    @XmlAttribute(name = "strategy")
    protected GenerationType strategy;
    @XmlAttribute(name = "generator")
    protected String generator;
    
    public GenerationType getStrategy() {
        return this.strategy;
    }
    
    public void setStrategy(final GenerationType value) {
        this.strategy = value;
    }
    
    public String getGenerator() {
        return this.generator;
    }
    
    public void setGenerator(final String value) {
        this.generator = value;
    }
}
