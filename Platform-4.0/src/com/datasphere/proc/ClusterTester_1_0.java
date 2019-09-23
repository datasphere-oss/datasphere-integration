package com.datasphere.proc;

import com.datasphere.proc.events.*;
import org.apache.log4j.*;
import java.util.*;

import com.datasphere.anno.*;
import com.datasphere.event.*;

import org.joda.time.*;

@PropertyTemplate(name = "ClusterTester", type = AdapterType.internal, properties = { @PropertyTemplateProperty(name = "keyPrefix", type = String.class, required = false, defaultValue = "KEY-"), @PropertyTemplateProperty(name = "numKeys", type = int.class, required = false, defaultValue = "10"), @PropertyTemplateProperty(name = "numEvents", type = int.class, required = false, defaultValue = "100"), @PropertyTemplateProperty(name = "intMin", type = int.class, required = false, defaultValue = "1"), @PropertyTemplateProperty(name = "intMax", type = int.class, required = false, defaultValue = "1"), @PropertyTemplateProperty(name = "doubleMin", type = double.class, required = false, defaultValue = "1"), @PropertyTemplateProperty(name = "doubleMax", type = double.class, required = false, defaultValue = "1"), @PropertyTemplateProperty(name = "tsDelta", type = long.class, required = false, defaultValue = "1000"), @PropertyTemplateProperty(name = "sequential", type = boolean.class, required = false, defaultValue = "false"), @PropertyTemplateProperty(name = "realtime", type = boolean.class, required = false, defaultValue = "false") }, inputType = ClusterTestEvent.class)
public class ClusterTester_1_0 extends SourceProcess
{
    private static Logger logger;
    String keyPrefix;
    int numKeys;
    int numEvents;
    int intMin;
    int intMax;
    int intRange;
    double doubleMin;
    double doubleMax;
    double doubleRange;
    long tsDelta;
    boolean sequential;
    boolean realtime;
    boolean reporteos;
    int count;
    int currKey;
    int sample;
    long currTs;
    Random rand;
    
    public ClusterTester_1_0() {
        this.count = 0;
        this.currKey = 1;
        this.currTs = 0L;
        this.rand = new Random(1627235253L);
    }
    
    private String getProp(final Map<String, Object> properties, final String name) {
        Object o = properties.get(name);
        if (o == null) {
            o = properties.get(name.toLowerCase());
        }
        return (o == null) ? "" : o.toString();
    }
    
    @Override
    public void init(final Map<String, Object> properties) throws Exception {
        super.init(properties);
        this.keyPrefix = this.getProp(properties, "keyPrefix");
        this.numKeys = Integer.parseInt(this.getProp(properties, "numKeys"));
        this.numEvents = Integer.parseInt(this.getProp(properties, "numEvents"));
        this.intMin = Integer.parseInt(this.getProp(properties, "intMin"));
        this.intMax = Integer.parseInt(this.getProp(properties, "intMax"));
        this.intRange = this.intMax - this.intMin;
        this.doubleMin = Double.parseDouble(this.getProp(properties, "doubleMin"));
        this.doubleMax = Double.parseDouble(this.getProp(properties, "doubleMax"));
        this.doubleRange = this.doubleMax - this.doubleMin;
        this.tsDelta = Long.parseLong(this.getProp(properties, "tsDelta"));
        this.sequential = Boolean.parseBoolean(this.getProp(properties, "sequential"));
        this.realtime = Boolean.parseBoolean(this.getProp(properties, "realtime"));
        this.reporteos = Boolean.parseBoolean(this.getProp(properties, "reporteos"));
        this.sample = this.numEvents / 100;
    }
    
    @Override
    public void receiveImpl(final int channel, final Event event) throws Exception {
        if (this.numEvents == -1 || this.count < this.numEvents) {
            if (this.realtime) {
                Thread.sleep(this.tsDelta);
                this.currTs = System.currentTimeMillis();
            }
            else if (this.currTs == 0L) {
                this.currTs = System.currentTimeMillis();
            }
            else {
                this.currTs += this.tsDelta;
            }
            final ClusterTestEvent t = new ClusterTestEvent(this.currTs);
            if (this.sequential) {
                t.keyValue = this.keyPrefix + this.currKey++;
                if (this.currKey > this.numKeys) {
                    this.currKey = 1;
                }
                int val;
                if (this.intRange >= 0) {
                    val = this.intMin + this.count % (this.intRange + 1);
                }
                else {
                    val = this.intMin + this.count;
                }
                t.intValue = val;
                t.doubleValue = val;
            }
            else {
                t.keyValue = this.keyPrefix + (this.rand.nextInt(this.numKeys) + 1);
                t.intValue = ((this.intRange > 0) ? (this.rand.nextInt() * this.intRange) : 0) + this.intMin;
                t.doubleValue = ((this.doubleRange > 0.0) ? (this.rand.nextDouble() * this.doubleRange) : 0.0) + this.doubleMin;
            }
            t.ts = new DateTime(this.currTs);
            this.send((Event)t, 0);
        }
        else {
            Thread.sleep(1000L);
        }
        ++this.count;
        if (this.count == this.numEvents && this.reporteos && ClusterTester_1_0.logger.isDebugEnabled()) {
            ClusterTester_1_0.logger.debug((Object)("ClusterTester finished output of " + this.numEvents + "  events"));
        }
    }
    
    public static void main(final String[] args) {
    }
    
    static {
        ClusterTester_1_0.logger = Logger.getLogger((Class)ClusterTester_1_0.class);
    }
}
