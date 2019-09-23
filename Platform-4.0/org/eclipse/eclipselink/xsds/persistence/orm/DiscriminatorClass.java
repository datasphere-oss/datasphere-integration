package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "discriminator-class")
public class DiscriminatorClass
{
    @XmlAttribute(name = "discriminator", required = true)
    protected String discriminator;
    @XmlAttribute(name = "value", required = true)
    protected String value;
    
    public String getDiscriminator() {
        return this.discriminator;
    }
    
    public void setDiscriminator(final String value) {
        this.discriminator = value;
    }
    
    public String getValue() {
        return this.value;
    }
    
    public void setValue(final String value) {
        this.value = value;
    }
}
