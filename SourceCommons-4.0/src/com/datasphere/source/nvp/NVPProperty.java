package com.datasphere.source.nvp;

import java.util.*;

import com.datasphere.source.lib.prop.*;

public class NVPProperty extends Property
{
    public static final String RECORD_DELIMITER = "nvprecorddelimiter";
    public static final String VALUE_DELIMITER = "nvpvaluedelimiter";
    public static final String DEFAULT_RECORD_DELIMITER = " ";
    public static final String DEFAULT_VALUE_DELIMITER = "=";
    public static final String DEFAULT_RECORD_DELIMITER_SPLIT_CHAR = ":";
    public String header;
    public char commentcharacter;
    public int columnDelimitTill;
    public String valueDelimiter;
    public String[] recordDelimiterList;
    
    public NVPProperty(final Map<String, Object> mp) {
        super(mp);
        String delimiterSplitChar = (String)mp.get("separator");
        if (delimiterSplitChar == null) {
            delimiterSplitChar = ":";
        }
        String recDel = (String)mp.get("nvprecorddelimiter");
        if (recDel == null) {
            recDel = " ";
        }
        this.nvprecorddelimiter = recDel.split(delimiterSplitChar);
        recDel = (String)mp.get("nvpvaluedelimiter");
        if (recDel == null) {
            recDel = "=";
        }
        this.nvpvaluedelimiter = recDel.split(delimiterSplitChar);
    }
}
