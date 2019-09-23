package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;
import java.util.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "fetch-group", propOrder = { "attribute" })
public class FetchGroup
{
    @XmlElement(required = true)
    protected List<FetchAttribute> attribute;
    @XmlAttribute(name = "name", required = true)
    protected String name;
    @XmlAttribute(name = "load")
    protected Boolean load;
    
    public List<FetchAttribute> getAttribute() {
        if (this.attribute == null) {
            this.attribute = new ArrayList<FetchAttribute>();
        }
        return this.attribute;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String value) {
        this.name = value;
    }
    
    public Boolean isLoad() {
        return this.load;
    }
    
    public void setLoad(final Boolean value) {
        this.load = value;
    }
}
