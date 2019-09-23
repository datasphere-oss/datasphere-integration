package com.datasphere.proc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.anno.PropertyTemplateProperty;
import com.datasphere.event.Event;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.events.StringArrayEvent;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;

@PropertyTemplate(name = "ranReader", type = AdapterType.internal, properties = { @PropertyTemplateProperty(name = "OutputType", type = String.class, required = true, defaultValue = ""), @PropertyTemplateProperty(name = "NoLimit", type = String.class, required = true, defaultValue = "false"), @PropertyTemplateProperty(name = "SampleSize", type = Integer.class, required = true, defaultValue = "-1"), @PropertyTemplateProperty(name = "DataKey", type = String.class, required = true, defaultValue = ""), @PropertyTemplateProperty(name = "NumberOfUniqueKeys", type = Integer.class, required = true, defaultValue = "0"), @PropertyTemplateProperty(name = "TimeInterval", type = Integer.class, required = false, defaultValue = "0") }, inputType = StringArrayEvent.class)
public class RandomReader extends SourceProcess
{
    private static Logger logger;
    boolean infinite;
    int sampleSize;
    static String keyInput;
    int uniqueKeys;
    int ss;
    public static StringArrayEvent out;
    static String[] sampArray;
    int tempSize;
    int counter;
    int ranNumber;
    int numOfUK;
    static String useKey;
    private long interval_in_microsec;
    static List<String> keyList;
    static List<Object> valList;
    MDRepository cache;
    String[] dummy;
    int recordsForEachKey;
    
    public RandomReader() {
        this.cache = MetadataRepository.getINSTANCE();
        this.dummy = null;
        this.recordsForEachKey = 1;
    }
    
    @Override
    public synchronized void init(final Map<String, Object> properties) throws Exception {
        this.infinite = false;
        this.sampleSize = 0;
        RandomReader.keyInput = "";
        this.uniqueKeys = 0;
        this.ss = 0;
        this.tempSize = 0;
        this.counter = 0;
        this.ranNumber = 0;
        this.numOfUK = 0;
        RandomReader.useKey = "";
        this.interval_in_microsec = 0L;
        RandomReader.keyList = new ArrayList<String>();
        RandomReader.valList = new ArrayList<Object>();
        this.cache = MetadataRepository.getINSTANCE();
        this.dummy = null;
        this.recordsForEachKey = 1;
        super.init(properties);
        this.infinite = (properties.containsKey("NoLimit") && properties.get("NoLimit").toString().equalsIgnoreCase("true"));
        final Object ssize = properties.get("SampleSize");
        if (ssize != null) {
            this.sampleSize = Integer.parseInt(ssize.toString());
        }
        RandomReader.keyInput = (String)properties.get("DataKey");
        final Object interval = properties.get("TimeInterval");
        if (interval != null) {
            this.interval_in_microsec = Long.parseLong(interval.toString());
        }
        final String typeName = (String)properties.get("OutputType");
        final String[] namespaceAndName = typeName.split("\\.");
        final MetaInfo.Type t = (MetaInfo.Type)this.cache.getMetaObjectByName(EntityType.TYPE, namespaceAndName[0], namespaceAndName[1], null, HSecurityManager.TOKEN);
        for (final Map.Entry<String, String> attr : t.fields.entrySet()) {
            RandomReader.keyList.add(attr.getKey());
            RandomReader.valList.add(attr.getValue());
        }
        RandomReader.sampArray = new String[RandomReader.valList.size()];
        if (!this.infinite) {
            if (!RandomReader.keyList.contains(RandomReader.keyInput)) {
                throw new Exception("Key does not exist");
            }
            if (RandomReader.keyInput.isEmpty()) {
                this.uniqueKeys = this.sampleSize;
            }
            else {
                this.uniqueKeys = (int)properties.get("NumberOfUniqueKeys");
            }
            if (this.uniqueKeys > this.sampleSize) {
                throw new Exception("NumberOfUniqueKeys is out of bounds (must be less than sampleSize)");
            }
            if (this.uniqueKeys <= 0) {
                this.uniqueKeys = this.sampleSize;
            }
            this.tempSize = this.sampleSize - this.uniqueKeys;
        }
        final int stringArraySize = RandomReader.keyList.size();
        this.dummy = new String[stringArraySize];
    }
    
    @Override
    public synchronized void close() throws Exception {
        super.close();
    }
    
    @Override
    public synchronized void receiveImpl(final int channel, final Event event) throws Exception {
        RandomReader.out = new StringArrayEvent(System.currentTimeMillis());
        RandomReader.out.data = this.dummy;
        if (this.ss < this.sampleSize || this.infinite) {
            if (this.infinite) {
                InfFields();
            }
            else if (this.counter == 0 && this.numOfUK != this.uniqueKeys) {
                RanFields();
                this.ranNumber = Util.ranInt(Util.nRandom, 0, this.tempSize);
                this.tempSize -= this.ranNumber;
                this.counter = this.ranNumber;
                ++this.ss;
                ++this.numOfUK;
            }
            else {
                keySpecificFields();
                ++this.ss;
                --this.counter;
            }
            if (this.interval_in_microsec > 0L) {
                final long delay = this.interval_in_microsec * 1000L;
                final long start = System.nanoTime();
                while (System.nanoTime() < start + delay) {}
            }
            final int myC = 0;
            this.send((Event)RandomReader.out, 0);
        }
        else {
            Thread.sleep(1000L);
        }
    }
    
    public static void InfFields() {
        for (int i = 0; i < RandomReader.valList.size(); ++i) {
            if (RandomReader.valList.get(i).equals("java.lang.String")) {
                RandomReader.out.data[i] = Util.ranString(Util.nRandom, "aeiou bcdfghj", 9);
            }
            else if (RandomReader.valList.get(i).equals("int")) {
                final int temp = Util.ranInt(Util.nRandom, 20000, 20100);
                RandomReader.out.data[i] = Integer.toString(temp);
            }
            else if (RandomReader.valList.get(i).equals("double") || RandomReader.valList.get(i).equals("java.lang.Double")) {
                final double temp2 = Util.ranInt(Util.nRandom, 10000, 999999) / 1000.0;
                RandomReader.out.data[i] = String.valueOf(temp2);
            }
        }
    }
    
    public static void RanFields() {
        for (int i = 0; i < RandomReader.valList.size(); ++i) {
            if (RandomReader.valList.get(i).equals("java.lang.String")) {
                RandomReader.out.data[i] = Util.ranString(Util.nRandom, "aeiou bcdfghj", 9);
                RandomReader.sampArray[i] = RandomReader.out.data[i];
                if (RandomReader.keyList.get(i).equalsIgnoreCase(RandomReader.keyInput)) {
                    RandomReader.useKey = RandomReader.out.data[i];
                }
            }
            else if (RandomReader.valList.get(i).equals("int")) {
                final int temp = Util.ranInt(Util.nRandom, 20000, 20100);
                RandomReader.out.data[i] = Integer.toString(temp);
                RandomReader.sampArray[i] = RandomReader.out.data[i];
                if (RandomReader.keyList.get(i).equalsIgnoreCase(RandomReader.keyInput)) {
                    RandomReader.useKey = RandomReader.out.data[i];
                }
            }
            else if (RandomReader.valList.get(i).equals("double") || RandomReader.valList.get(i).equals("java.lang.Double")) {
                final double temp2 = Util.ranInt(Util.nRandom, 10000, 999999) / 1000.0;
                RandomReader.out.data[i] = String.valueOf(temp2);
                RandomReader.sampArray[i] = RandomReader.out.data[i];
                if (RandomReader.keyList.get(i).equalsIgnoreCase(RandomReader.keyInput)) {
                    RandomReader.useKey = RandomReader.out.data[i];
                }
            }
        }
    }
    
    public static void keySpecificFields() {
        for (int i = 0; i < RandomReader.valList.size(); ++i) {
            if (!RandomReader.keyList.get(i).equalsIgnoreCase(RandomReader.keyInput)) {
                RandomReader.out.data[i] = RandomReader.sampArray[i];
            }
            else {
                RandomReader.out.data[i] = RandomReader.useKey;
            }
        }
    }
    
    public static void main(final String[] args) {
        final Map<String, Object> props = new HashMap<String, Object>();
        props.put("NoLimit", "false");
        props.put("SampleSize", "100");
        props.put("DataKey", "2");
        props.put("NumberOfUniqueKeys", "10");
        final RandomReader mrr = new RandomReader();
        try {
            mrr.init(props);
            mrr.addEventSink(new AbstractEventSink() {
                @Override
                public void receive(final int channel, final Event event) {
                    final int eventLength = ((Object[])event.getPayload()[0]).length;
                    if (RandomReader.logger.isDebugEnabled()) {
                        RandomReader.logger.debug((Object)("EventLength : " + eventLength));
                    }
                    String data = "";
                    for (int i = 0; i < eventLength; ++i) {
                        data = data + (String)((Object[])event.getPayload()[0])[i] + ',';
                    }
                    if (RandomReader.logger.isDebugEnabled()) {
                        RandomReader.logger.debug((Object)data);
                    }
                }
            });
            for (int i = 0; i < 10000; ++i) {
                mrr.receive(0, null);
            }
        }
        catch (Exception e) {
            RandomReader.logger.error((Object)e);
        }
    }
    
    static {
        RandomReader.logger = Logger.getLogger((Class)RandomReader.class);
    }
}
