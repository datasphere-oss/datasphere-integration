package com.datasphere.runtime;

import org.apache.commons.lang.builder.*;

public class LocationInfo
{
    public double latVal;
    public double longVal;
    public String city;
    public String zip;
    public String companyName;
    
    public LocationInfo() {
        this.latVal = 0.0;
        this.longVal = 0.0;
        this.city = null;
        this.zip = null;
        this.companyName = null;
    }
    
    LocationInfo(final double latVal, final double longVal, final String city, final String zip, final String companyName) {
        this.latVal = 0.0;
        this.longVal = 0.0;
        this.city = null;
        this.zip = null;
        this.companyName = null;
        this.latVal = latVal;
        this.longVal = longVal;
        this.city = city;
        this.zip = zip;
        this.companyName = companyName;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (other instanceof LocationInfo) {
            final LocationInfo li = (LocationInfo)other;
            if (this.latVal == li.latVal && this.longVal == li.longVal) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.latVal).append(this.longVal).toHashCode();
    }
    
    @Override
    public String toString() {
        return super.toString();
    }
}
