package com.datasphere.preview;

import org.codehaus.jettison.json.*;

public class DataFile
{
    public long size;
    public String name;
    public String fqPath;
    String type;
    
    public JSONObject toJson() throws JSONException {
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("Size", this.size);
        jsonObject.put("Name", (Object)this.name);
        jsonObject.put("Path", (Object)this.fqPath);
        jsonObject.put("Type", (Object)this.type);
        return jsonObject;
    }
    
    @Override
    public String toString() {
        return "FQP of " + this.name + " is " + this.fqPath + " with a size: " + this.size;
    }
}
