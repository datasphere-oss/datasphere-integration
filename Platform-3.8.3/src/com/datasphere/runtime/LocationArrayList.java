package com.datasphere.runtime;

import java.util.*;

public class LocationArrayList
{
    public ArrayList<LocationInfo> value;
    
    public void add(final LocationInfo locationInfo) {
        synchronized (this.value) {
            this.value.add(locationInfo);
        }
    }
    
    public void remove(final LocationInfo locationInfo) {
        synchronized (this.value) {
            this.value.remove(locationInfo);
        }
    }
    
    public LocationArrayList(final LocationArrayList other) {
        this.value = new ArrayList<LocationInfo>(other.value);
    }
    
    public LocationArrayList() {
        this.value = new ArrayList<LocationInfo>();
    }
}
