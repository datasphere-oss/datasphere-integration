package org.eclipse.eclipselink.xsds.persistence.orm;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "no-sql")
public class NoSql
{
    @XmlAttribute(name = "data-type")
    protected String dataType;
    @XmlAttribute(name = "data-format")
    protected DataFormatType dataFormat;
    
    public String getDataType() {
        return this.dataType;
    }
    
    public void setDataType(final String value) {
        this.dataType = value;
    }
    
    public DataFormatType getDataFormat() {
        return this.dataFormat;
    }
    
    public void setDataFormat(final DataFormatType value) {
        this.dataFormat = value;
    }
}
