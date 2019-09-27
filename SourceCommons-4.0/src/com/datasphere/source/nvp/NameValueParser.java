package com.datasphere.source.nvp;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.common.constants.*;
import java.io.*;

public class NameValueParser
{
    private final int MAX_COL_COUNT = 2;
    private final int NAME_COLUMN = 0;
    private final int VALUE_COLUMN = 1;
    private NVPResultSet resultSet;
    static Logger logger;
    NVPProperty prop;
    
    public NameValueParser(final NVPProperty prop) {
        this.prop = prop;
    }
    
    public Map<String, String> convertToMap(final String msg) {
        String additionalValue = "";
        String prevName = "";
        boolean invalidMsg = false;
        final Map<String, String> map = new HashMap<String, String>();
        try {
            if (this.resultSet == null) {
                this.resultSet = new NVPResultSet(this.prop);
            }
            this.resultSet.setBuffer(msg);
            while (this.resultSet.next() == Constant.recordstatus.VALID_RECORD) {
                if (this.resultSet.getColumnCount() == 2) {
                    String value = "";
                    String colName = this.resultSet.getColumnValue(0);
                    String colValue = this.resultSet.getColumnValue(1);
                    if (additionalValue.length() != 0) {
                        if (prevName != null && prevName.length() != 0) {
                            value = map.get(prevName);
                            value += additionalValue;
                            map.put(prevName, value);
                            additionalValue = "";
                        }
                        else {
                            additionalValue = "";
                        }
                    }
                    colName = colName.trim();
                    colValue = colValue.trim();
                    map.put(colName, colValue);
                    prevName = colName;
                }
                else if (this.resultSet.getColumnCount() > 2) {
                    if (!invalidMsg) {
                        NameValueParser.logger.error((Object)("Error while parsing Name Value Pair for [" + msg + "]"));
                        invalidMsg = true;
                    }
                    for (int idx = 0; idx < this.resultSet.getColumnCount(); ++idx) {
                        additionalValue = additionalValue + " " + this.resultSet.getColumnValue(idx);
                        additionalValue = additionalValue.trim();
                    }
                }
                else {
                    if (this.resultSet.getColumnValue(0).length() == 0) {
                        continue;
                    }
                    additionalValue = additionalValue + " " + this.resultSet.getColumnValue(0);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e2) {
            e2.printStackTrace();
        }
        if (additionalValue.length() != 0) {
            String value = map.get(prevName);
            value = value + " " + additionalValue;
            map.put(prevName, value);
        }
        return map;
    }
    
    static {
        NameValueParser.logger = Logger.getLogger((Class)NameValueParser.class);
    }
}
