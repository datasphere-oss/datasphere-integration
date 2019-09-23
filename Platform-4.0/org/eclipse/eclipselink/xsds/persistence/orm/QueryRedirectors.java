package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "query-redirectors")
public class QueryRedirectors
{
    @XmlAttribute(name = "all-queries")
    protected String allQueries;
    @XmlAttribute(name = "read-all")
    protected String readAll;
    @XmlAttribute(name = "read-object")
    protected String readObject;
    @XmlAttribute(name = "report")
    protected String report;
    @XmlAttribute(name = "update")
    protected String update;
    @XmlAttribute(name = "insert")
    protected String insert;
    @XmlAttribute(name = "delete")
    protected String delete;
    
    public String getAllQueries() {
        return this.allQueries;
    }
    
    public void setAllQueries(final String value) {
        this.allQueries = value;
    }
    
    public String getReadAll() {
        return this.readAll;
    }
    
    public void setReadAll(final String value) {
        this.readAll = value;
    }
    
    public String getReadObject() {
        return this.readObject;
    }
    
    public void setReadObject(final String value) {
        this.readObject = value;
    }
    
    public String getReport() {
        return this.report;
    }
    
    public void setReport(final String value) {
        this.report = value;
    }
    
    public String getUpdate() {
        return this.update;
    }
    
    public void setUpdate(final String value) {
        this.update = value;
    }
    
    public String getInsert() {
        return this.insert;
    }
    
    public void setInsert(final String value) {
        this.insert = value;
    }
    
    public String getDelete() {
        return this.delete;
    }
    
    public void setDelete(final String value) {
        this.delete = value;
    }
}
