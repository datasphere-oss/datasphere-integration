package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "clone-copy-policy")
public class CloneCopyPolicy
{
    @XmlAttribute(name = "method")
    protected String method;
    @XmlAttribute(name = "working-copy-method")
    protected String workingCopyMethod;
    
    public String getMethod() {
        return this.method;
    }
    
    public void setMethod(final String value) {
        this.method = value;
    }
    
    public String getWorkingCopyMethod() {
        return this.workingCopyMethod;
    }
    
    public void setWorkingCopyMethod(final String value) {
        this.workingCopyMethod = value;
    }
}
