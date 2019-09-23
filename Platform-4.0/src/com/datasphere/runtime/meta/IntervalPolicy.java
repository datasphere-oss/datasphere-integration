package com.datasphere.runtime.meta;

import com.fasterxml.jackson.annotation.*;
import com.datasphere.runtime.*;
import java.util.*;
import java.io.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.*;

public class IntervalPolicy implements Serializable
{
    private static final long serialVersionUID = 5645537094795872797L;
    public Kind kind;
    public CountBasedPolicy countBasedPolicy;
    public TimeBasedPolicy timeBasedPolicy;
    public AttrBasedPolicy attrBasedPolicy;
    
    public IntervalPolicy() {
    }
    
    public IntervalPolicy(final Kind kind, final CountBasedPolicy cp, final TimeBasedPolicy tp, final AttrBasedPolicy ap) {
        this.kind = kind;
        this.countBasedPolicy = cp;
        this.timeBasedPolicy = tp;
        this.attrBasedPolicy = ap;
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        if (this.getCountPolicy() != null && this.getCountPolicy().getCountInterval() == -1) {
            sb.append(" IDLE TIMEOUT " + this.getTimePolicy());
            return sb.toString();
        }
        if (this.kind != null) {
            sb.append(this.kind.toString());
        }
        if (this.isTimeBased()) {
            sb.append(" " + this.getTimePolicy());
        }
        if (this.isCountBased()) {
            sb.append(" " + this.getCountPolicy());
        }
        if (this.isAttrBased()) {
            sb.append(" " + this.getAttrPolicy());
        }
        return sb.toString();
    }
    
    @JsonIgnore
    public CountBasedPolicy getCountPolicy() {
        return this.countBasedPolicy;
    }
    
    @JsonIgnore
    public TimeBasedPolicy getTimePolicy() {
        return this.timeBasedPolicy;
    }
    
    @JsonIgnore
    public AttrBasedPolicy getAttrPolicy() {
        return this.attrBasedPolicy;
    }
    
    @JsonIgnore
    public boolean isCountBased() {
        return this.getCountPolicy() != null;
    }
    
    @JsonIgnore
    public boolean isTimeBased() {
        return this.getTimePolicy() != null;
    }
    
    @JsonIgnore
    public boolean isAttrBased() {
        return this.getAttrPolicy() != null;
    }
    
    public Kind getKind() {
        return this.kind;
    }
    
    private static CountBasedPolicy makeCountPolicy(final int count) {
        return new CountBasedPolicy(count);
    }
    
    private static TimeBasedPolicy makeTimePolicy(final Interval time) {
        return new TimeBasedPolicy(time);
    }
    
    private static AttrBasedPolicy makeAttrPolicy(final String name, final long range) {
        return new AttrBasedPolicy(name, range);
    }
    
    private static IntervalPolicy makePolicy(final Kind kind, final CountBasedPolicy cp, final TimeBasedPolicy tp, final AttrBasedPolicy ap) {
        return new IntervalPolicy(kind, cp, tp, ap);
    }
    
    public static IntervalPolicy createCountPolicy(final int count) {
        return makePolicy(Kind.COUNT, makeCountPolicy(count), null, null);
    }
    
    public static IntervalPolicy createAttrPolicy(final String name, final long range) {
        return makePolicy(Kind.ATTR, null, null, makeAttrPolicy(name, range));
    }
    
    public static IntervalPolicy createTimePolicy(final Interval time) {
        return makePolicy(Kind.TIME, null, makeTimePolicy(time), null);
    }
    
    public static IntervalPolicy createTimeCountPolicy(final Interval time, final int count) {
        return makePolicy(Kind.TIME_COUNT, makeCountPolicy(count), makeTimePolicy(time), null);
    }
    
    public static IntervalPolicy createTimeAttrPolicy(final Interval time, final String name, final long range) {
        return makePolicy(Kind.TIME_ATTR, null, makeTimePolicy(time), makeAttrPolicy(name, range));
    }
    
    @JsonIgnore
    public boolean isCount1() {
        return this.getKind() == Kind.COUNT && this.getCountPolicy().getCountInterval() == 1;
    }
    
    @JsonIgnore
    public static Kind getKindFromString(final String kind) {
        if (kind.equals("COUNT")) {
            return Kind.COUNT;
        }
        if (kind.equals("ATTR")) {
            return Kind.ATTR;
        }
        if (kind.equals("TIME")) {
            return Kind.TIME;
        }
        if (kind.equals("TIME_COUNT")) {
            return Kind.TIME_COUNT;
        }
        if (kind.equals("TIME_ATTR")) {
            return Kind.TIME_ATTR;
        }
        return null;
    }
    
    public static IntervalPolicy deserialize(final JsonNode jsonNode) throws IOException, JsonProcessingException {
        if (jsonNode == null) {
            return null;
        }
        Integer countSize = null;
        Interval timeLimit = null;
        List<String> fields = null;
        final JsonNode ctNode = jsonNode.get("count_interval");
        if (ctNode != null) {
            countSize = (ctNode.toString().equalsIgnoreCase("null") ? null : new Integer(ctNode.asInt()));
        }
        final JsonNode tiNode = jsonNode.get("time_interval");
        if (tiNode != null) {
            final JsonNode valNode = tiNode.get("value");
            if (valNode != null) {
                timeLimit = (valNode.toString().equalsIgnoreCase("null") ? null : new Interval(valNode.asLong()));
            }
        }
        final JsonNode optionalAttrNode = jsonNode.get("optional_attr");
        if (optionalAttrNode != null && optionalAttrNode.isArray() && optionalAttrNode.get(0) != null) {
            fields = new ArrayList<String>();
            fields.add(optionalAttrNode.get(0).asText());
        }
        if (countSize != null && timeLimit != null) {
            return createTimeCountPolicy(timeLimit, countSize);
        }
        if (fields != null && !fields.isEmpty() && timeLimit != null) {
            return createAttrPolicy(fields.get(0), timeLimit.value);
        }
        if (countSize != null) {
            return createCountPolicy(countSize);
        }
        if (timeLimit != null) {
            return createTimePolicy(timeLimit);
        }
        return null;
    }
    
    public static class CountBasedPolicy implements Serializable
    {
        private static final long serialVersionUID = -8721320131044552238L;
        public int count;
        
        public CountBasedPolicy() {
        }
        
        public CountBasedPolicy(final int cnt) {
            this.count = cnt;
        }
        
        @JsonIgnore
        public int getCountInterval() {
            return this.count;
        }
        
        @Override
        public String toString() {
            return this.count + " ROWS";
        }
    }
    
    public static class TimeBasedPolicy implements Serializable
    {
        private static final long serialVersionUID = 7377334123916855969L;
        public Interval time;
        
        public TimeBasedPolicy() {
        }
        
        public TimeBasedPolicy(final Interval time) {
            this.time = time;
        }
        
        @JsonIgnore
        public long getTimeInterval() {
            return this.time.value;
        }
        
        @Override
        public String toString() {
            return "WITHIN " + this.time.toHumanReadable();
        }
    }
    
    public static class AttrBasedPolicy implements Serializable
    {
        private static final long serialVersionUID = 6109839268916036301L;
        public long range;
        public String name;
        
        public AttrBasedPolicy() {
        }
        
        public AttrBasedPolicy(final String name, final long range) {
            this.range = range;
            this.name = name;
        }
        
        @JsonIgnore
        public long getAttrValueRange() {
            return this.range;
        }
        
        @JsonIgnore
        public String getAttrName() {
            return this.name;
        }
        
        @Override
        public String toString() {
            return "RANGE " + this.range + " ON " + this.name;
        }
    }
    
    public enum Kind
    {
        COUNT, 
        TIME, 
        ATTR, 
        TIME_COUNT, 
        TIME_ATTR;
    }
    
    public static class IntervalPolicyDeserializer extends StdDeserializer<IntervalPolicy>
    {
        public IntervalPolicyDeserializer() {
            super((Class)IntervalPolicy.class);
        }
        
        public IntervalPolicy deserialize(final JsonParser jp, final DeserializationContext dctx) throws IOException, JsonProcessingException {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.setConfig(dctx.getConfig());
            jp.setCodec((ObjectCodec)mapper);
            if (jp.hasCurrentToken()) {
                final JsonNode winPolacyIntNode = (JsonNode)jp.readValueAsTree();
                return IntervalPolicy.deserialize(winPolacyIntNode);
            }
            return null;
        }
    }
}
