package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.apache.log4j.*;

import com.datasphere.event.*;
import com.fasterxml.jackson.databind.*;

import org.eclipse.persistence.sessions.*;
import java.util.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.meta.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class IntervalPolicyConverter implements Converter
{
    private static final long serialVersionUID = -8049064931525422511L;
    private static Logger logger;
    ObjectMapper mapper;
    
    public IntervalPolicyConverter() {
        this.mapper = ObjectMapperFactory.newInstance();
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session arg1) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String) {
            try {
                final HashMap<String, Object> iPolicyMap = (HashMap<String, Object>)this.mapper.readValue((String)dataValue, (Class)HashMap.class);
                int countVal = -1;
                long timeVal = -1L;
                String attrNameVal = null;
                long attrRangeVal = -1L;
                IntervalPolicy objVal = null;
                final Object attrPolicy = iPolicyMap.get("attrBasedPolicy");
                if (attrPolicy != null) {
                    final HashMap<String, Object> attrPolicyHash = (HashMap<String, Object>)attrPolicy;
                    attrNameVal = (String)attrPolicyHash.get("name");
                    final Object attrRangeValObj = attrPolicyHash.get("range");
                    if (attrRangeValObj instanceof Long) {
                        attrRangeVal = (long)attrRangeValObj;
                    }
                    else {
                        attrRangeVal = (int)attrRangeValObj;
                    }
                }
                final Object countPolicy = iPolicyMap.get("countBasedPolicy");
                if (countPolicy != null) {
                    final HashMap<String, Integer> countPolicyHash = (HashMap<String, Integer>)countPolicy;
                    countVal = countPolicyHash.get("count");
                }
                final Object timePolicy = iPolicyMap.get("timeBasedPolicy");
                if (timePolicy != null) {
                    final HashMap<String, Object> timePolicyHash = (HashMap<String, Object>)timePolicy;
                    final HashMap<String, Object> timeValHash = (HashMap<String, Object>)timePolicyHash.get("time");
                    final Object timeValObj = timeValHash.get("value");
                    if (timeValObj instanceof Long) {
                        timeVal = (long)timeValObj;
                    }
                    else {
                        timeVal = (int)timeValObj;
                    }
                }
                if (attrPolicy != null && timePolicy != null) {
                    objVal = IntervalPolicy.createTimeAttrPolicy(new Interval(timeVal), attrNameVal, attrRangeVal);
                }
                else if (countPolicy != null && timePolicy != null) {
                    objVal = IntervalPolicy.createTimeCountPolicy(new Interval(timeVal), countVal);
                }
                else if (countPolicy != null) {
                    objVal = IntervalPolicy.createCountPolicy(countVal);
                }
                else if (timePolicy != null) {
                    objVal = IntervalPolicy.createTimePolicy(new Interval(timeVal));
                }
                else if (attrPolicy != null) {
                    objVal = IntervalPolicy.createAttrPolicy(attrNameVal, attrRangeVal);
                }
                return objVal;
            }
            catch (Exception e) {
                IntervalPolicyConverter.logger.error((Object)("Problem reading Interval policy " + dataValue + " from JSON"));
            }
        }
        return null;
    }
    
    public Object convertObjectValueToDataValue(final Object iPolicy, final Session arg1) {
        if (iPolicy == null) {
            return null;
        }
        if (iPolicy instanceof IntervalPolicy) {
            try {
                return this.mapper.writeValueAsString(iPolicy);
            }
            catch (Exception e) {
                IntervalPolicyConverter.logger.error((Object)("Problem writing Interval policy " + iPolicy + " as JSON"));
            }
        }
        return null;
    }
    
    public void initialize(final DatabaseMapping arg0, final Session arg1) {
    }
    
    public boolean isMutable() {
        return false;
    }
    
    static {
        IntervalPolicyConverter.logger = Logger.getLogger((Class)IntervalPolicyConverter.class);
    }
}
