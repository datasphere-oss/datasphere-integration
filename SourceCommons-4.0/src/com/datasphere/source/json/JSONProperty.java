package com.datasphere.source.json;

import java.util.*;

import com.datasphere.source.lib.prop.*;

public class JSONProperty extends Property
{
    public String type;
    Map<String, Object> map;
    
    @Override
    public Map<String, Object> getMap() {
        return this.map;
    }
    
    public JSONProperty(final Map<String, Object> mp) {
        super(mp);
        this.map = mp;
        if (mp.get("wildcard") == null) {
            this.wildcard = "*.json";
        }
        else {
            this.wildcard = (String)mp.get("wildcard");
        }
        if (mp.get("portno") != null) {
            this.portno = this.getInt(mp, "portno");
        }
        else {
            this.portno = 0;
        }
    }
}
