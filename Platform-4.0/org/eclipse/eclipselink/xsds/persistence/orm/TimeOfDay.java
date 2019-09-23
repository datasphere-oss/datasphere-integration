package org.eclipse.eclipselink.xsds.persistence.orm;

import java.math.*;
import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "time-of-day")
public class TimeOfDay
{
    @XmlAttribute(name = "hour")
    protected BigInteger hour;
    @XmlAttribute(name = "minute")
    protected BigInteger minute;
    @XmlAttribute(name = "second")
    protected BigInteger second;
    @XmlAttribute(name = "millisecond")
    protected BigInteger millisecond;
    
    public BigInteger getHour() {
        return this.hour;
    }
    
    public void setHour(final BigInteger value) {
        this.hour = value;
    }
    
    public BigInteger getMinute() {
        return this.minute;
    }
    
    public void setMinute(final BigInteger value) {
        this.minute = value;
    }
    
    public BigInteger getSecond() {
        return this.second;
    }
    
    public void setSecond(final BigInteger value) {
        this.second = value;
    }
    
    public BigInteger getMillisecond() {
        return this.millisecond;
    }
    
    public void setMillisecond(final BigInteger value) {
        this.millisecond = value;
    }
}
