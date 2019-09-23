package com.datasphere.runtime.components;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.datasphere.event.SimpleEvent;
import com.datasphere.event.HDConvertible;
import com.datasphere.exception.ESTypeCannotCastException;
import com.datasphere.exception.SecurityException;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.proc.events.DynamicEvent;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.containers.DynamicEventWrapper;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HD;
import com.datasphere.hdstore.Utility;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class CQSubTask implements Runnable
{
    private static final Logger logger;
    private static final RecordKey defaultKey;
    public CQTask context;
    private ITaskEvent taskEvent;
    private boolean flagIsAdd;
    private DARecord curEvent;
    private IRemoveDuplicates dupRemover;
    private IGroupByPolicy grouper;
    private IOrderByPolicy sorter;
    private IOffsetAndLimit limiter;
    private static final IOrderByPolicy notOrdered;
    private static final IRemoveDuplicates doRemove;
    private static final IRemoveDuplicates doNotRemove;
    private static final IOffsetAndLimit noLimits;
    
    public abstract void setRow(final DARecord p0);
    
    public abstract void init(final CQTask p0);
    
    public abstract Object[] getSourceEvents();
    
    public abstract Object getRowData(final int p0);
    
    public abstract Position getRowPosition(final int p0);
    
    public abstract Object getWindowRow(final int p0, final int p1);
    
    public abstract int getThisDS();
    
    public abstract void runImpl();
    
    public abstract void updateState();
    
    @Override
    public abstract void run();
    
    public final void setEvent(final ITaskEvent event) {
        this.taskEvent = event;
    }
    
    public final boolean isAdd() {
        return this.flagIsAdd;
    }
    
    public final Object genNextInt() {
        return this.sorter.genNextInt();
    }
    
    public final void setGroupByKeyAndAggVec(final RecordKey groupByKey) {
        final Object[] aggVec = this.context.getAggVec(groupByKey, this.isAdd());
        this.grouper.setGroupByKey(groupByKey, aggVec);
    }
    
    public final void setGroupByKeyAndDefaultAggVec() {
        this.setGroupByKeyAndAggVec(CQSubTask.defaultKey);
    }
    
    public final Object getAggVec(final int index) {
        return this.grouper.getAggVec(index);
    }
    
    public IBatch getAdded() {
        return this.taskEvent.batch();
    }
    
    public IBatch getAddedFiltered(final RecordKey key) {
        return this.taskEvent.filterBatch(0, key);
    }
    
    public IBatch getRemoved() {
        return this.taskEvent.removedBatch();
    }
    
    private final void processBatch(final IBatch batch) {
    		Iterator iter = batch.iterator();
        while (iter.hasNext()) {
        	final DARecord e = (DARecord)iter.next();
            this.setRow(e);
            try {
                this.runImpl();
            }
            catch (ESTypeCannotCastException e2) {
                if (!CQSubTask.logger.isInfoEnabled()) {
                    continue;
                }
                CQSubTask.logger.info((Object)e2);
            }
        }
    }
    
    private final void processAdded() {
        this.flagIsAdd = true;
        if (this.atLeastOneDSisStreaming() && this.isNotOuterJoin() && !this.allStreamingSourcesReady()) {
            return;
        }
        this.processBatch(this.getAdded());
    }
    
    boolean atLeastOneDSisStreaming() {
        return this.context.atLeastOneDSisStreaming();
    }
    
    public boolean isNotOuterJoin() {
        return false;
    }
    
    boolean allStreamingSourcesReady() {
        return this.context.allStreamingSourcesReady();
    }
    
    private final void processRemoved() {
        this.flagIsAdd = false;
        this.processBatch(this.getRemoved());
    }
    
    public final void processAggregated() {
        final Position batchPosition = this.buildBatchPosition((Iterable<DARecord>)this.getAdded());
        this.createResultBatch();
        this.processAdded();
        this.processRemoved();
        final List<DARecord> xnew = this.getResultBatch();
        for (final DARecord e : xnew) {
            e.position = batchPosition;
        }
        this.context.doOutput(xnew, Collections.emptyList());
    }
    
    public final void processNotAggregated() {
        this.createResultBatch();
        this.processAdded();
        final List<DARecord> xnew = this.getResultBatch();
        this.createResultBatch();
        this.processRemoved();
        final List<DARecord> xold = this.getResultBatch();
        for (final DARecord e : xnew) {
            if (e.position != null) {
                e.position = e.position.createAugmentedPosition(this.context.getMetaID(), (String)null);
            }
        }
        this.context.doOutput(xnew, xold);
    }
    
    public final void setOutputEvent(final Object data) {
        final DARecord event = new DARecord(data, this.getRowPosition(this.getThisDS()));
        this.curEvent = event;
    }
    
    public final void removeGroupKey() {
        this.grouper.removeGroupKey();
    }
    
    public final void initResultBatchBuilder(final IGroupByPolicy grouper, final IOrderByPolicy sorter, final IRemoveDuplicates dupRemover, final IOffsetAndLimit limiter) {
        this.dupRemover = dupRemover;
        this.grouper = grouper;
        this.sorter = sorter;
        this.limiter = limiter;
    }
    
    private final void createResultBatch() {
        this.grouper.createBatch();
    }
    
    public final void setOrderByKey(final RecordKey key) {
        this.sorter.setOrderByKey(key);
    }
    
    public final void linkSourceEvents() {
        if (this.curEvent.data instanceof SimpleEvent) {
            final SimpleEvent se = (SimpleEvent)this.curEvent.data;
            this.grouper.addSourceEvents(se, this, this.sorter);
        }
    }
    
    public final void addEvent() {
        final Object eventWithKey = this.sorter.makeEventWithOrderByKey(this.curEvent);
        this.grouper.addEvent(eventWithKey);
    }
    
    private final List<DARecord> getResultBatch() {
        final Collection<Object> b = this.grouper.getResultBatch();
        final Collection<Object> b2 = this.dupRemover.removeDuplicates(b);
        final Collection<DARecord> b3 = this.sorter.sort(b2);
        final Collection<DARecord> b4 = this.limiter.limitOutput(b3);
        final List<DARecord> res = makeList(b4);
        return res;
    }
    
    private Position buildBatchPosition(final Iterable<DARecord> events) {
        if (!this.context.recoveryIsEnabled()) {
            return null;
        }
        PathManager result = null;
        for (final DARecord e : events) {
            if (e.position == null) {
                continue;
            }
            if (result == null) {
                result = new PathManager();
            }
            result.mergeWiderPosition(e.position.values());
        }
        if (result != null) {
            result.augment(this.context.getMetaID(), (String)null);
        }
        return (result != null) ? result.toPosition() : null;
    }
    
    public void caseNotFound() {
        throw new RuntimeException("Case not found");
    }
    
    public static boolean doLike(final String s, final Pattern p) {
        if (s == null || p == null) {
            return false;
        }
        final Matcher m = p.matcher(s);
        return m.matches();
    }
    
    public Object getRowDataAndTranslate(final Object obj, final int ds) throws MetaDataRepositoryException {
        final DynamicEvent e = (DynamicEvent)obj;
        final TranslatedSchema schema = this.context.getTranslatedSchema(e.getEventType(), ds);
        return new DynamicEventWrapper(e, schema);
    }
    
    public Object getDynamicField(final DynamicEventWrapper e, final int fieldIndex, final int dataSetIndex) {
        try {
            final int translatedIndex = e.schema.realFieldIndex[fieldIndex];
            return (translatedIndex < 0) ? null : e.event.data[translatedIndex];
        }
        catch (ArrayIndexOutOfBoundsException ex) {
            return null;
        }
    }
    
    public boolean checkDynamicRecType(final DynamicEventWrapper e) {
        final boolean isTypeExpected = e.schema.instanceOfExpectedType;
        return isTypeExpected;
    }
    
    private static Object castJsonTo(final JsonNode fld, final Class<?> type) {
        if (fld.isNull()) {
            return null;
        }
        if (Number.class.isAssignableFrom(type)) {
            if (type == Integer.class) {
                return fld.asInt();
            }
            if (type == Long.class) {
                return fld.asLong();
            }
            if (type == Short.class) {
                return (short)fld.asInt();
            }
            if (type == Byte.class) {
                return (byte)fld.asInt();
            }
            if (type == Double.class) {
                return fld.asDouble();
            }
            if (type == Float.class) {
                return (float)fld.asDouble();
            }
        }
        else {
            if (Date.class.isAssignableFrom(type)) {
                final String val = fld.asText();
                return Timestamp.valueOf(val);
            }
            if (DateTime.class.isAssignableFrom(type)) {
                final Long val2 = fld.asLong();
                return new DateTime((long)val2);
            }
            if (type == Boolean.class) {
                return fld.asBoolean();
            }
            if (type == String.class) {
                return fld.asText();
            }
            if (type == UUID.class) {
                return new UUID(fld.textValue());
            }
            if (type.isAssignableFrom(JsonNode.class)) {
                return fld;
            }
            if (type == Object.class) {
                return fld;
            }
            if (type.getCanonicalName().equalsIgnoreCase("java.lang.Object[]")) {
                final ArrayNode arrayNode = (ArrayNode)fld;
                final Object[] resultArray = new Object[arrayNode.size()];
                final Iterator<JsonNode> iterator = (Iterator<JsonNode>)arrayNode.iterator();
                int i = 0;
                while (iterator.hasNext()) {
                    resultArray[i++] = iterator.next().asText();
                }
                return resultArray;
            }
            if (type.getCanonicalName().equalsIgnoreCase("java.util.HashMap")) {
                final Map<String, Object> result = (Map<String, Object>)Utility.objectMapper.convertValue((Object)fld, (Class)Map.class);
                return result;
            }
            if (type.getCanonicalName().equalsIgnoreCase("byte[]")) {
                final byte[] result2 = (byte[])Utility.objectMapper.convertValue((Object)fld, (Class)byte[].class);
                return result2;
            }
        }
        throw new RuntimeException("cannot convert json object " + fld + " to object of type " + type.getCanonicalName());
    }
    
    public static Object dynamicCast(final JsonNode obj, final String fieldName, final Class<?> type) {
        final ObjectNode hd = (ObjectNode)obj;
        final JsonNode fld = hd.get(fieldName);
        if (fld == null) {
            throw new ESTypeCannotCastException("cannot access field <" + fieldName + "> in event " + obj);
        }
        return castJsonTo(fld, type);
    }
    
    public CQTask getContext() {
        return this.context;
    }
    
    public Object convertWA2Ctx(final Object o, final Class<?> klazz) {
        final HD wa = (HD)o;
        try {
            final SimpleEvent ev = (SimpleEvent)klazz.newInstance();
            if (ev instanceof HDConvertible && wa != null) {
                ((HDConvertible)ev).convertFromDatallToRecord(wa.getHDTs(), wa.getUuid(), wa.getKeyString(), (Map)wa.getContext());
            }
            return ev;
        }
        catch (InstantiationException | IllegalAccessException ex2) {
            throw new RuntimeException(ex2);
        }
    }
    
    public Object getWARowContext(final Object obj, final Class<?> klazz) {
        return this.convertWA2Ctx(obj, klazz);
    }
    
    private static String toText(final Object o) {
        return o.toString();
    }
    
    private static Number toNum(final Object o) {
        return (o instanceof Number) ? ((Number)o) : new BigDecimal(toText(o));
    }
    
    private static Boolean toBool(final Object o) {
        return (Boolean)((o instanceof Boolean) ? o : Boolean.valueOf(toText(o)));
    }
    
    public Object getParamVal(final int index, final String pname, final Class<?> expectedType) {
        final Object val = this.context.getParam(index);
        if (val == null) {
            return null;
        }
        if (expectedType == Object.class) {
            return val;
        }
        if (Number.class.isAssignableFrom(expectedType)) {
            if (expectedType == Integer.class) {
                return toNum(val).intValue();
            }
            if (expectedType == Long.class) {
                return toNum(val).longValue();
            }
            if (expectedType == Short.class) {
                return toNum(val).shortValue();
            }
            if (expectedType == Byte.class) {
                return toNum(val).byteValue();
            }
            if (expectedType == Double.class) {
                return toNum(val).doubleValue();
            }
            if (expectedType == Float.class) {
                return toNum(val).floatValue();
            }
        }
        else {
            if (Date.class.isAssignableFrom(expectedType)) {
                final String tmp = toText(val);
                return Timestamp.valueOf(tmp);
            }
            if (DateTime.class.isAssignableFrom(expectedType)) {
                final Long tmp2 = toNum(val).longValue();
                return new DateTime((long)tmp2);
            }
            if (expectedType == Boolean.class) {
                return toBool(val);
            }
            if (expectedType == String.class) {
                return toText(val);
            }
            if (expectedType == UUID.class) {
                return new UUID(toText(val));
            }
        }
        throw new RuntimeException("cannot convert parameter " + pname + " to type " + expectedType.getCanonicalName());
    }
    
    public static Object getJavaObjectField(final Object obj, final String fieldName) {
        try {
            final Class<?> objType = obj.getClass();
            final Field fld = objType.getField(fieldName);
            final Object fldVal = fld.get(obj);
            return fldVal;
        }
        catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex2) {
        		return null;
        }
    }
    
    public static Object getJsonObjectField(final JsonNode obj, final String fieldName, final Class<?> type) {
        final JsonNode fldVal = obj.get(fieldName);
        return (fldVal != null) ? castJsonTo(fldVal, type) : null;
    }
    
    private static <T> List<T> makeList(final Collection<T> col) {
        List<T> ret;
        if (col instanceof List) {
            ret = (List<T>)(List)col;
        }
        else {
            ret = new ArrayList<T>((Collection<? extends T>)col);
        }
        return ret;
    }
    
    public static IGroupByPolicy makeGrouped() {
        return new Grouped();
    }
    
    public static IGroupByPolicy makeNotGrouped() {
        return new NotGrouped();
    }
    
    public static IOrderByPolicy makeOrdered(final boolean sortasc) {
        return new Ordered(sortasc);
    }
    
    public static IOrderByPolicy makeNotOrdered() {
        return CQSubTask.notOrdered;
    }
    
    public static IRemoveDuplicates makeRemoveDups() {
        return CQSubTask.doRemove;
    }
    
    public static IRemoveDuplicates makeNotRemoveDups() {
        return CQSubTask.doNotRemove;
    }
    
    public static IOffsetAndLimit makeLimited(final int offset, final int limit) {
        return new HasOffsetAndLimit(offset, limit);
    }
    
    public static IOffsetAndLimit makeNotLimited() {
        return CQSubTask.noLimits;
    }
    
    public abstract void cleanState();
    
    static {
        logger = Logger.getLogger((Class)CQSubTask.class);
        defaultKey = new RecordKey(new Object[0]);
        notOrdered = new IOrderByPolicy() {
            @Override
            public void setOrderByKey(final RecordKey key) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public int genNextInt() {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public Object makeEventWithOrderByKey(final DARecord event) {
                return event;
            }
            
            @Override
            public DARecord getEventWithoutKey(final Object o) {
                return (DARecord)o;
            }
            
            @Override
            public Collection<DARecord> sort(final Collection<?> col) {
                return (Collection<DARecord>)col;
            }
            
            @Override
            public String toString() {
                return "not ordered";
            }
        };
        doRemove = new IRemoveDuplicates() {
            @Override
            public Collection<Object> removeDuplicates(final Collection<Object> col) {
                return new LinkedHashSet<Object>(col);
            }
            
            @Override
            public String toString() {
                return "remove duplicates";
            }
        };
        doNotRemove = new IRemoveDuplicates() {
            @Override
            public Collection<Object> removeDuplicates(final Collection<Object> col) {
                return col;
            }
            
            @Override
            public String toString() {
                return "do not remove duplicates";
            }
        };
        noLimits = new IOffsetAndLimit() {
            @Override
            public Collection<DARecord> limitOutput(final Collection<DARecord> col) {
                return col;
            }
            
            @Override
            public String toString() {
                return "not limited";
            }
        };
    }
    
    private static class Ordered implements IOrderByPolicy
    {
        private static final Comparator<RecordKey> comparator;
        private static final Comparator<RecordKey> reverse_comparator;
        private int nextInt;
        private RecordKey orderByKey;
        private final boolean sortasc;
        
        Ordered(final boolean sortasc) {
            this.sortasc = sortasc;
        }
        
        @Override
        public void setOrderByKey(final RecordKey key) {
            this.orderByKey = key;
        }
        
        @Override
        public int genNextInt() {
            return this.nextInt++;
        }
        
        @Override
        public Object makeEventWithOrderByKey(final DARecord event) {
            return new OrderKeyAndEvent(this.orderByKey, event);
        }
        
        @Override
        public DARecord getEventWithoutKey(final Object o) {
            final OrderKeyAndEvent p = (OrderKeyAndEvent)o;
            return (p == null) ? null : p.event;
        }
        
        @Override
        public Collection<DARecord> sort(final Collection<?> col) {
            return this.sortList((Collection<OrderKeyAndEvent>)col, this.sortasc ? Ordered.comparator : Ordered.reverse_comparator);
        }
        
        private Collection<DARecord> sortList(final Collection<OrderKeyAndEvent> col, final Comparator<RecordKey> cmp) {
            final Map<RecordKey, DARecord> index = new TreeMap<RecordKey, DARecord>(cmp);
            for (final OrderKeyAndEvent e : col) {
                index.put(e.key, e.event);
            }
            final Collection<DARecord> ret = index.values();
            return ret;
        }
        
        @Override
        public String toString() {
            return "ordered " + (this.sortasc ? "asc" : "desc");
        }
        
        static {
            comparator = new Comparator<RecordKey>() {
                @Override
                public int compare(final RecordKey k1, final RecordKey k2) {
                    return k1.compareTo(k2);
                }
            };
            reverse_comparator = Collections.reverseOrder(Ordered.comparator);
        }
    }
    
    private static class NotGrouped implements IGroupByPolicy
    {
        private List<Object> batch;
        
        @Override
        public void addSourceEvents(final SimpleEvent se, final CQSubTask owner, final IOrderByPolicy sorter) {
            if (owner.isAdd()) {
                final List<Object[]> lst = Collections.singletonList(owner.getSourceEvents());
                se.setSourceEvents((List)lst);
            }
        }
        
        @Override
        public void setGroupByKey(final RecordKey key, final Object[] aggVec) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public Object getAggVec(final int index) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void removeGroupKey() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void addEvent(final Object o) {
            this.batch.add(o);
        }
        
        @Override
        public void createBatch() {
            this.batch = new ArrayList<Object>();
        }
        
        @Override
        public Collection<Object> getResultBatch() {
            return this.batch;
        }
        
        @Override
        public String toString() {
            return "not grouped";
        }
    }
    
    private static class Grouped implements IGroupByPolicy
    {
        private RecordKey groupByKey;
        private Object[] aggVec;
        private Map<Object, Object> batch;
        
        @Override
        public void addSourceEvents(final SimpleEvent se, final CQSubTask owner, final IOrderByPolicy sorter) {
            List<Object[]> lst = null;
            final DARecord wae = sorter.getEventWithoutKey(this.batch.get(this.groupByKey));
            if (wae != null) {
                final SimpleEvent pe = (SimpleEvent)wae.data;
                lst = (List<Object[]>)pe.linkedSourceEvents;
            }
            if (owner.isAdd()) {
                if (lst == null) {
                    lst = new ArrayList<Object[]>(2);
                }
                lst.add(owner.getSourceEvents());
            }
            se.setSourceEvents((List)lst);
        }
        
        @Override
        public void setGroupByKey(final RecordKey key, final Object[] aggVec) {
            this.groupByKey = key;
            this.aggVec = aggVec.clone();
        }
        
        @Override
        public Object getAggVec(final int index) {
            return this.aggVec[index];
        }
        
        @Override
        public void removeGroupKey() {
            this.batch.remove(this.groupByKey);
        }
        
        @Override
        public void addEvent(final Object o) {
            this.batch.put(this.groupByKey, o);
        }
        
        @Override
        public void createBatch() {
            this.batch = new LinkedHashMap<Object, Object>();
        }
        
        @Override
        public Collection<Object> getResultBatch() {
            return this.batch.values();
        }
        
        @Override
        public String toString() {
            return "grouped";
        }
    }
    
    private static class HasOffsetAndLimit implements IOffsetAndLimit
    {
        private final int offset;
        private final int limit;
        
        HasOffsetAndLimit(final int offset, final int limit) {
            this.offset = offset;
            this.limit = limit;
        }
        
        @Override
        public Collection<DARecord> limitOutput(final Collection<DARecord> col) {
            final int size = col.size();
            if (this.offset == 0) {
                if (size <= this.limit) {
                    return col;
                }
                return getSubList(col, 0, this.limit);
            }
            else {
                if (this.offset >= size) {
                    return Collections.emptyList();
                }
                final int lim = this.offset + this.limit;
                if (size <= lim) {
                    return getSubList(col, this.offset, size);
                }
                return getSubList(col, this.offset, lim);
            }
        }
        
        private static Collection<DARecord> getSubList(final Collection<DARecord> col, final int begin, final int end) {
            if (col instanceof List) {
                return (Collection<DARecord>)((List)col).subList(begin, end);
            }
            final List<DARecord> res = new ArrayList<DARecord>();
            int i = 0;
            for (final DARecord e : col) {
                if (i >= begin) {
                    if (i >= end) {
                        break;
                    }
                    res.add(e);
                }
                ++i;
            }
            return res;
        }
        
        @Override
        public String toString() {
            return "limit " + this.limit + " offset " + this.offset;
        }
    }
    
    private static class OrderKeyAndEvent
    {
        public final RecordKey key;
        public final DARecord event;
        
        public OrderKeyAndEvent(final RecordKey key, final DARecord event) {
            this.key = key;
            this.event = event;
        }
        
        @Override
        public final boolean equals(final Object o) {
            if (!(o instanceof OrderKeyAndEvent)) {
                return false;
            }
            final OrderKeyAndEvent other = (OrderKeyAndEvent)o;
            return this.event.equals((Object)other.event);
        }
        
        @Override
        public final int hashCode() {
            return this.event.hashCode();
        }
        
        @Override
        public final String toString() {
            return "(" + this.key + "," + this.event + ")";
        }
    }
    
    private interface IOffsetAndLimit
    {
        Collection<DARecord> limitOutput(final Collection<DARecord> p0);
    }
    
    private interface IRemoveDuplicates
    {
        Collection<Object> removeDuplicates(final Collection<Object> p0);
    }
    
    private interface IOrderByPolicy
    {
        void setOrderByKey(final RecordKey p0);
        
        Object makeEventWithOrderByKey(final DARecord p0);
        
        DARecord getEventWithoutKey(final Object p0);
        
        Collection<DARecord> sort(final Collection<?> p0);
        
        int genNextInt();
    }
    
    private interface IGroupByPolicy
    {
        void setGroupByKey(final RecordKey p0, final Object[] p1);
        
        void removeGroupKey();
        
        void addSourceEvents(final SimpleEvent p0, final CQSubTask p1, final IOrderByPolicy p2);
        
        void addEvent(final Object p0);
        
        void createBatch();
        
        Collection<Object> getResultBatch();
        
        Object getAggVec(final int p0);
    }
}
