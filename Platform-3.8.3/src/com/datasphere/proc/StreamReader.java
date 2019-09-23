package com.datasphere.proc;

import org.apache.log4j.*;
import com.datasphere.proc.events.*;
import org.joda.time.*;
import com.datasphere.metaRepository.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.components.*;
import com.datasphere.security.*;
import com.datasphere.anno.*;
import com.datasphere.event.*;

import java.util.*;

@PropertyTemplate(name = "StreamReader", type = AdapterType.internal, properties = { @PropertyTemplateProperty(name = "OutputType", type = String.class, required = true, defaultValue = ""), @PropertyTemplateProperty(name = "noLimit", type = String.class, required = true, defaultValue = "false"), @PropertyTemplateProperty(name = "isSeeded", type = String.class, required = false, defaultValue = "false"), @PropertyTemplateProperty(name = "maxRows", type = Integer.class, required = false, defaultValue = "0"), @PropertyTemplateProperty(name = "increment", type = Double.class, required = false, defaultValue = "1.0"), @PropertyTemplateProperty(name = "iterations", type = Integer.class, required = false, defaultValue = "0"), @PropertyTemplateProperty(name = "iterationDelay", type = Integer.class, required = false, defaultValue = "0"), @PropertyTemplateProperty(name = "StringSet", type = String.class, required = false, defaultValue = ""), @PropertyTemplateProperty(name = "NumberSet", type = String.class, required = false, defaultValue = "") }, inputType = ObjectArrayEvent.class)
public class StreamReader extends SourceProcess
{
    private Logger logger;
    int maxRows;
    int iterations;
    long iterationDelay;
    String dataRange;
    public StringArrayEvent out;
    public ObjectArrayEvent out2;
    int objectIDcount;
    int irc;
    int count;
    boolean infinite;
    String objects;
    int objectListCount;
    String typeName;
    String noLimit;
    String strSet;
    DateTime dt;
    int strCounter;
    String dataType;
    int listSize;
    double Lmin;
    double increment;
    RandomGen generator;
    String isSeeded;
    int m_r;
    long r_t;
    List<String> nameList;
    List<Object> dataTypeList;
    ArrayList<String> setElements;
    Map<String, boundaries> dataRanges;
    Map<String, ArrayList<String>> strSetMap;
    MDRepository cache;
    public String[] eachTypeBound;
    public String[] nameMinMaxDis;
    public String[] strList;
    public String[] nameArray;
    String namespace;
    
    public StreamReader() {
        this.logger = Logger.getLogger((Class)StreamReader.class);
        this.cache = MetadataRepository.getINSTANCE();
        this.namespace = "";
    }
    
    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }
    
    @Override
    public synchronized void init(final Map<String, Object> properties) throws Exception {
        this.dt = new DateTime();
        this.noLimit = "";
        this.maxRows = 0;
        this.iterations = 0;
        this.iterationDelay = 0L;
        this.dataRange = "";
        this.objectIDcount = 1;
        this.irc = 0;
        this.count = 0;
        this.infinite = false;
        this.objects = "";
        this.objectListCount = 0;
        this.typeName = "";
        this.strSet = "";
        this.strCounter = 0;
        this.dataType = "";
        this.Lmin = 0.0;
        this.increment = 1.0;
        this.listSize = 0;
        this.m_r = 0;
        this.r_t = 0L;
        this.nameList = new ArrayList<String>();
        this.dataTypeList = new ArrayList<Object>();
        this.dataRanges = new HashMap<String, boundaries>();
        this.strSetMap = new HashMap<String, ArrayList<String>>();
        final MDRepository cache = MetadataRepository.getINSTANCE();
        this.eachTypeBound = new String[0];
        this.nameMinMaxDis = new String[0];
        this.strList = new String[0];
        this.nameArray = new String[0];
        super.init(properties);
        final Map<String, Object> localCopyOfProperty = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        localCopyOfProperty.putAll(properties);
        this.maxRows = this.getIntValue(localCopyOfProperty.get("maxRows"));
        this.noLimit = (String)localCopyOfProperty.get("noLimit");
        this.iterations = this.getIntValue(localCopyOfProperty.get("iterations"));
        this.iterationDelay = this.getLongValue(localCopyOfProperty.get("iterationDelay"));
        this.typeName = (String)localCopyOfProperty.get("OutputType");
        this.dataRange = (String)localCopyOfProperty.get("NumberSet");
        this.strSet = (String)localCopyOfProperty.get("StringSet");
        this.isSeeded = (String)localCopyOfProperty.get("isSeeded");
        this.increment = (Double)localCopyOfProperty.get("increment");
        if (this.isSeeded.equalsIgnoreCase("true")) {
            this.generator = new RandomGen(1);
        }
        else {
            this.generator = new RandomGen();
        }
        if (this.strSet != null) {
            this.strList = this.strSet.split(",");
            if (this.strList.length > 0) {
                this.nameArray = this.strList[0].replaceAll("\\s+", "").split("\\[|\\-|\\]");
                this.listSize = this.nameArray.length - 1;
            }
            for (int i = 0; i < this.strList.length; ++i) {
                this.nameArray = this.strList[i].replaceAll("\\s+", "").split("\\[|\\-|\\]");
                this.setElements = new ArrayList<String>();
                for (int j = 1; j < this.nameArray.length; ++j) {
                    this.setElements.add(this.nameArray[j]);
                }
                this.strSetMap.put(this.nameArray[0], this.setElements);
            }
        }
        try {
            MetaInfo.Type t;
            if (this.typeName.contains(".")) {
                final String[] namespaceAndName = this.typeName.split("\\.");
                t = (MetaInfo.Type)cache.getMetaObjectByName(EntityType.TYPE, namespaceAndName[0], namespaceAndName[1], null, HSecurityManager.TOKEN);
            }
            else {
                t = (MetaInfo.Type)cache.getMetaObjectByName(EntityType.TYPE, this.namespace, this.typeName.trim(), null, HSecurityManager.TOKEN);
            }
            for (final Map.Entry<String, String> attr : t.fields.entrySet()) {
                this.nameList.add(attr.getKey());
                this.dataTypeList.add(attr.getValue());
            }
        }
        catch (NullPointerException e) {
            throw new NullPointerException("Incorrect namespace for TYPE: " + this.typeName + "\nCheck format in Source");
        }
        if (this.dataRange != null) {
            this.eachTypeBound = this.dataRange.split(",");
            for (int i = 0; i < this.eachTypeBound.length; ++i) {
                this.nameMinMaxDis = this.eachTypeBound[i].replaceAll("\\s+", "").split("\\[|\\-|\\]");
                boundaries a;
                if (this.nameMinMaxDis.length == 3) {
                    a = new boundaries(Double.parseDouble(this.nameMinMaxDis[1]), Double.parseDouble(this.nameMinMaxDis[1]), this.nameMinMaxDis[2]);
                    if (this.nameMinMaxDis[2].equalsIgnoreCase("Linc")) {
                        final boundaries boundaries = a;
                        --boundaries.point;
                    }
                    else if (this.nameMinMaxDis[2].equalsIgnoreCase("Ldec")) {
                        final boundaries boundaries2 = a;
                        ++boundaries2.point;
                    }
                }
                else {
                    a = new boundaries(Double.parseDouble(this.nameMinMaxDis[1]), Double.parseDouble(this.nameMinMaxDis[2]), this.nameMinMaxDis[3]);
                    if (this.nameMinMaxDis[3].equalsIgnoreCase("Linc")) {
                        final boundaries boundaries3 = a;
                        --boundaries3.point;
                    }
                    else if (this.nameMinMaxDis[3].equalsIgnoreCase("Ldec")) {
                        final boundaries boundaries4 = a;
                        ++boundaries4.point;
                    }
                }
                this.dataRanges.put(this.nameMinMaxDis[0], a);
                if (this.strSet == null) {
                    this.listSize = this.eachTypeBound.length;
                }
            }
        }
    }
    
    public int getIntValue(final Object data) {
        if (data instanceof Integer) {
            return (int)data;
        }
        return (int)data;
    }
    
    public long getLongValue(final Object data) {
        if (data instanceof Integer) {
            return (long)data;
        }
        return (long)data;
    }
    
    @Override
    public synchronized void close() throws Exception {
        super.close();
    }
    
    @Override
    public synchronized void receiveImpl(final int channel, final Event event) throws Exception {
        this.out2 = new ObjectArrayEvent(System.currentTimeMillis());
        this.out2.data = new Object[this.nameList.size()];
        if (this.noLimit.equalsIgnoreCase("true")) {
            this.setFields();
            if (this.iterationDelay > 0L) {
                try {
                    Thread.sleep(this.iterationDelay);
                }
                catch (InterruptedException e) {
                    this.logger.warn((Object)e);
                }
            }
            this.send((Event)this.out2, 0);
            if (this.objectListCount == this.listSize - 1) {
                this.objectListCount = 0;
            }
            else {
                ++this.objectListCount;
            }
        }
        if (this.irc < this.iterations && this.noLimit.equalsIgnoreCase("false")) {
            this.setFields();
            this.send((Event)this.out2, 0);
            if (this.iterationDelay > 0L && this.objectListCount + 1 == this.listSize) {
                try {
                    Thread.sleep(this.iterationDelay);
                }
                catch (InterruptedException e) {
                    this.logger.warn((Object)e);
                }
            }
            if (this.objectListCount == this.listSize - 1) {
                this.objectListCount = 0;
                ++this.irc;
            }
            else {
                ++this.objectListCount;
            }
        }
        if (this.m_r < this.maxRows && this.noLimit.equalsIgnoreCase("false")) {
            this.setFields();
            if (this.objectListCount == this.listSize - 1) {
                this.objectListCount = 0;
            }
            else {
                ++this.objectListCount;
            }
            ++this.m_r;
            if (this.iterationDelay > 0L) {
                try {
                    Thread.sleep(this.iterationDelay);
                }
                catch (InterruptedException e) {
                    this.logger.warn((Object)e);
                }
            }
            this.send((Event)this.out2, 0);
        }
    }
    
    public void setFields() throws Exception {
        for (int i = 0; i < this.nameList.size(); ++i) {
            this.dataType = String.valueOf(this.dataTypeList.get(i));
            try {
                if (this.dataType.equalsIgnoreCase("java.lang.String")) {
                    final ArrayList<String> strArr = this.strSetMap.get(this.nameList.get(i));
                    if (this.objectListCount < strArr.size()) {
                        this.out2.data[i] = strArr.get(this.objectListCount);
                    }
                }
                else if (this.dataType.equalsIgnoreCase("java.lang.Long") || this.dataType.equalsIgnoreCase("int") || this.dataType.equalsIgnoreCase("java.lang.Double") || this.dataType.equalsIgnoreCase("integer") || this.dataType.equalsIgnoreCase("java.lang.Integer")) {
                    final boundaries newb = this.dataRanges.get(this.nameList.get(i));
                    if (newb.dis.equalsIgnoreCase("G")) {
                        this.out2.data[i] = this.GDistribution(newb, this.dataType);
                    }
                    else if (newb.dis.equalsIgnoreCase("R")) {
                        this.out2.data[i] = this.RDistribution(newb, this.dataType);
                    }
                    else if (newb.dis.equalsIgnoreCase("L")) {
                        this.out2.data[i] = this.LDistribution(newb, this.dataType);
                    }
                    else if (newb.dis.equalsIgnoreCase("LInc")) {
                        this.out2.data[i] = this.LIncDistribution(newb, this.dataType);
                    }
                    else if (newb.dis.equalsIgnoreCase("LDec")) {
                        this.out2.data[i] = this.LDecDistribution(newb, this.dataType);
                    }
                }
                else {
                    if (!this.dataType.equalsIgnoreCase("org.joda.time.DateTime")) {
                        final String e = "Unsupported data type";
                        throw new Exception(e);
                    }
                    this.dt = DateTime.now();
                    this.out2.data[i] = this.dt;
                }
            }
            catch (NullPointerException xx) {
                throw new NullPointerException("Incorrect StringSet Name");
            }
        }
    }
    
    public static boolean isNumeric(final String str) {
        try {
            Double.parseDouble(str);
        }
        catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }
    
    public double LDistribution(final boundaries x, final String dT) {
        final int Rnumber = this.generator.nextIntInRange((int)x.min, (int)x.max);
        if (x.point < Rnumber) {
            x.point += this.increment;
        }
        else if (x.point >= Rnumber) {
            x.point -= this.increment;
        }
        if (dT.equalsIgnoreCase("int") || dT.equalsIgnoreCase("integer")) {
            return (int)Math.round(x.point);
        }
        if (dT.equalsIgnoreCase("long")) {
            return Math.round(x.point);
        }
        return x.point;
    }
    
    public double LIncDistribution(final boundaries x, final String dT) {
        x.point += this.increment;
        if (dT.equalsIgnoreCase("int") || dT.equalsIgnoreCase("integer")) {
            return (int)Math.round(x.point);
        }
        if (dT.equalsIgnoreCase("long")) {
            return Math.round(x.point);
        }
        return x.point;
    }
    
    public double LDecDistribution(final boundaries x, final String dT) {
        x.point -= this.increment;
        if (dT.equalsIgnoreCase("int") || dT.equalsIgnoreCase("integer")) {
            return (int)Math.round(x.point);
        }
        if (dT.equalsIgnoreCase("long")) {
            return Math.round(x.point);
        }
        return x.point;
    }
    
    public Object GDistribution(final boundaries x, final String dT) {
        final double mean = (x.min + x.max) / 2.0;
        final double SD = (x.max - mean) / 2.0;
        int delay;
        double Gnumber;
        do {
            Gnumber = mean + this.generator.nextGaussian() * SD;
            delay = (int)Math.round(Gnumber);
        } while (delay <= 0 || delay > x.max);
        if (dT.equalsIgnoreCase("int") || dT.equalsIgnoreCase("integer")) {
            return (int)Math.round(Gnumber);
        }
        if (dT.equalsIgnoreCase("long")) {
            return Math.round(Gnumber);
        }
        return Gnumber;
    }
    
    public Object RDistribution(final boundaries x, final String dT) {
        if (dT.equalsIgnoreCase("int") || dT.equalsIgnoreCase("integer")) {
            final int Rnumber = this.generator.nextIntInRange((int)x.min, (int)x.max);
            return Rnumber;
        }
        if (dT.equalsIgnoreCase("long")) {
            final double range = x.max - x.min + 1.0;
            final double frac = range * this.generator.nextDouble();
            return (long)(frac + x.min);
        }
        final double range = x.max - x.min + 1.0;
        final double frac = range * this.generator.nextDouble();
        return frac + x.min;
    }
    
    public void main(final String[] args) {
    }
    
    public static class RandomGen extends Random
    {
        public RandomGen(final int seed) {
            super(seed);
        }
        
        public RandomGen() {
        }
        
        public int nextIntInRange(final int min, final int max) {
            return this.nextInt(max - min + 1) + min;
        }
    }
    
    class boundaries
    {
        double min;
        double point;
        double max;
        String dis;
        
        boundaries(final double min, final double max, final String distribution) {
            this.min = 0.0;
            this.point = 0.0;
            this.max = 0.0;
            this.dis = "";
            this.min = min;
            this.point = min;
            this.max = max;
            this.dis = distribution;
        }
        
        boundaries() {
            this.min = 0.0;
            this.point = 0.0;
            this.max = 0.0;
            this.dis = "";
            this.min = 0.0;
            this.point = this.min;
            this.max = 0.0;
            this.dis = "R";
        }
        
        @Override
        public String toString() {
            return "min: " + this.min + " max: " + this.max + "dis: " + this.dis;
        }
    }
    
    static class Util
    {
        public static final Random nRandom;
        
        public String ranString(final Random rng, final String characters, final int length) {
            final char[] ranStr = new char[length];
            for (int i = 0; i < length; ++i) {
                ranStr[i] = characters.charAt(rng.nextInt(characters.length()));
            }
            return new String(ranStr);
        }
        
        public static int ranInt(final Random a, final double start, final double end) {
            final double range = end - start + 1.0;
            final double frac = range * a.nextDouble();
            final int ranNumber = (int)(frac + start);
            return ranNumber;
        }
        
        static {
            nRandom = new Random();
        }
    }
}
