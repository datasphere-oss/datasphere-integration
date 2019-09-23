package com.datasphere.runtime.monitor;

import org.apache.log4j.*;
import java.util.*;
import javax.management.*;

public class MainMBean implements DynamicMBean
{
    private Map<String, Object> metricMap;
    private static final Logger logger;
    
    public MainMBean() {
        this.metricMap = new HashMap<String, Object>();
    }
    
    public void updateMetric(final String metricName, final Object value) {
        this.metricMap.put(metricName, value);
    }
    
    @Override
    public Object getAttribute(final String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (!this.metricMap.containsKey(attribute)) {
            throw new AttributeNotFoundException("Cannot find attribute: " + attribute);
        }
        return this.metricMap.get(attribute);
    }
    
    @Override
    public void setAttribute(final Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        if (!this.metricMap.containsKey(attribute.getName())) {
            throw new AttributeNotFoundException();
        }
        this.metricMap.put(attribute.getName(), attribute.getValue());
    }
    
    @Override
    public AttributeList getAttributes(final String[] attributes) {
        final AttributeList attList = new AttributeList();
        for (final String attName : attributes) {
            if (this.metricMap.containsKey(attName)) {
                final Object val = this.metricMap.get(attName);
                final Attribute att = new Attribute(attName, val);
                attList.add(att);
            }
        }
        return attList;
    }
    
    @Override
    public AttributeList setAttributes(final AttributeList attributes) {
        final AttributeList attList = new AttributeList();
        for (final Attribute att : attributes.asList()) {
            if (this.metricMap.containsKey(att.getName())) {
                final Object val = att.getValue();
                this.metricMap.put(att.getName(), val);
                attList.add(att);
            }
        }
        return attList;
    }
    
    @Override
    public Object invoke(final String actionName, final Object[] params, final String[] signature) throws MBeanException, ReflectionException {
        return null;
    }
    
    @Override
    public MBeanInfo getMBeanInfo() {
        final String className = this.getClass().toString();
        final String description = "A Dynamic MBean storing monitoring metric names and values";
        final MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[this.metricMap.size()];
        int pos = 0;
        for (final Map.Entry<String, Object> entry : this.metricMap.entrySet()) {
            attributes[pos++] = new MBeanAttributeInfo(entry.getKey(), entry.getValue().getClass().getName(), MonitorEvent.Type.valueOf(entry.getKey()).getDescription(), true, false, false);
        }
        if (MainMBean.logger.isDebugEnabled()) {
            MainMBean.logger.debug((Object)("StriimMBean info - classname: " + className + " . Attributes: " + attributes));
        }
        return new MBeanInfo(className, description, attributes, null, null, null);
    }
    
    static {
        logger = Logger.getLogger((Class)MainMBean.class);
    }
}
