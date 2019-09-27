package com.datasphere.source.csv;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.source.lib.reader.Reader;
import com.datasphere.source.smlite.NVPEvent;
import com.datasphere.source.smlite.RowEvent;

public class NVPResultSet extends CSVResultSetLite
{
    Reader reader;
    private int anonymousColumnCount;
    public final int NAME_INDEX = 0;
    public final int VALUE_INDEX = 1;
    public final String KEY_NAME = "column";
    private String lastKeyName;
    private String pairDelimiter;
    private Map<String, Object> map;
    
    public NVPResultSet(final Reader reader, final Property prop) throws IOException, InterruptedException, AdapterException {
        super(reader, prop);
        this.anonymousColumnCount = 0;
        this.lastKeyName = null;
        this.pairDelimiter = null;
        this.reader = reader;
        this.map = new HashMap<String, Object>();
        this.pairDelimiter = prop.getString("pairdelimiter", " ");
    }
    
    @Override
    public void onEvent(final RowEvent rowEvent) {
        super.onEvent(rowEvent);
        this.populateNVPmap();
        this.recordBeginOffset = rowEvent.position();
        this.anonymousColumnCount = 0;
        this.lastKeyName = null;
    }
    
    @Override
    public void onEvent(final NVPEvent nvpEvent) {
        this.setColumnData(this.rEvent = nvpEvent);
        this.populateNVPmap();
        ++this.anonymousColumnCount;
    }
    
    private void populateNVPmap() {
        String name = null;
        String value = null;
        if (this.colCount == 2) {
            name = this.getColumnValue(0);
            value = this.getColumnValue(1);
        }
        else if (this.colCount == 1) {
            value = this.getColumnValue(0);
            if (this.lastKeyName == null || this.lastKeyName.contains("column")) {
                if (this.logger.isDebugEnabled()) {
                    this.logger.debug((Object)("Value [" + value + "] couln't associate with key, adding as anonymous column"));
                }
                name = "column" + this.anonymousColumnCount;
            }
            else {
                final String prevValue = (String)this.map.get(this.lastKeyName);
                name = this.lastKeyName;
                value = prevValue + this.pairDelimiter + value;
            }
        }
        if (name != null) {
            this.map.put(name, value);
        }
        this.lastKeyName = name;
        final int offset = this.columnBeginOffset[this.colCount];
        this.colCount = 0;
        this.columnBeginOffset[0] = offset;
    }
    
    public Map<String, Object> getNameValueMap() {
        return this.map;
    }
    
    public void clearEntries() {
        this.map.clear();
    }
}
