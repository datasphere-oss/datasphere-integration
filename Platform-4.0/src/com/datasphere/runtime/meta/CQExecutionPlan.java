package com.datasphere.runtime.meta;

import java.io.*;
import com.datasphere.runtime.compiler.select.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.*;
import flexjson.*;
import com.fasterxml.jackson.annotation.*;
import java.util.*;

public class CQExecutionPlan implements Serializable, Cloneable
{
    public static final int DUMP_INPUT = 1;
    public static final int DUMP_OUTPUT = 2;
    public static final int DUMP_CODE = 4;
    public static final int DUMP_PLAN = 8;
    public static final int DUMP_PROC = 16;
    public static final int DUMP_MULTILINE = 32;
    public static final int DEBUG_INFO = 64;
    public static final int IS_STATEFUL = 1;
    public static final int IS_MATCH = 2;
    private static final long serialVersionUID = 3505193508744220237L;
    public final List<RSFieldDesc> resultSetDesc;
    public final List<ParamDesc> paramsDesc;
    public final int kindOfOutputStream;
    public List<DataSource> dataSources;
    public final int indexCount;
    public final List<Class<?>> aggObjFactories;
    public final List<UUID> usedTypes;
    public final AggAlgo howToAggregate;
    public final TraceOptions traceOptions;
    public final int dataSetCount;
    public final int flags;
    @JSON(include = false)
    @JsonIgnore
    public final List<byte[]> code;
    @JSON(include = false)
    @JsonIgnore
    public final List<String> sourcecode;
    
    public CQExecutionPlan() {
        this.resultSetDesc = null;
        this.paramsDesc = null;
        this.kindOfOutputStream = 0;
        this.dataSources = null;
        this.code = null;
        this.indexCount = 0;
        this.aggObjFactories = null;
        this.usedTypes = null;
        this.howToAggregate = null;
        this.traceOptions = null;
        this.dataSetCount = 0;
        this.flags = 0;
        this.sourcecode = null;
    }
    
    private CQExecutionPlan(final CQExecutionPlan cq) throws CloneNotSupportedException {
        this.resultSetDesc = cq.resultSetDesc;
        this.paramsDesc = cq.paramsDesc;
        this.kindOfOutputStream = cq.kindOfOutputStream;
        this.dataSources = new ArrayList<DataSource>();
        for (final DataSource ds : cq.dataSources) {
            this.dataSources.add(ds.clone());
        }
        this.code = cq.code;
        this.indexCount = cq.indexCount;
        this.aggObjFactories = cq.aggObjFactories;
        this.usedTypes = cq.usedTypes;
        this.howToAggregate = cq.howToAggregate;
        this.traceOptions = cq.traceOptions;
        this.dataSetCount = cq.dataSetCount;
        this.flags = cq.flags;
        this.sourcecode = cq.sourcecode;
    }
    
    public CQExecutionPlan(final List<RSFieldDesc> rsdesc, final List<ParamDesc> paramsDesc, final int kindOfOutputStream, final List<DataSource> dataSources, final List<byte[]> code, final int indexCount, final List<Class<?>> aggObjFactories, final List<UUID> usedTypes, final boolean hasGroupBy, final boolean hasAggFunc, final TraceOptions traceOptions, final int dataSetCount, final boolean isStateful, final boolean isMatch, final List<String> sourcecodeList) {
        assert !code.isEmpty();
        this.resultSetDesc = rsdesc;
        this.paramsDesc = paramsDesc;
        this.kindOfOutputStream = kindOfOutputStream;
        this.dataSources = dataSources;
        this.code = code;
        this.indexCount = indexCount;
        this.aggObjFactories = aggObjFactories;
        this.usedTypes = usedTypes;
        this.howToAggregate = getAggAlgorithm(hasAggFunc, hasGroupBy);
        this.traceOptions = traceOptions;
        this.dataSetCount = dataSetCount;
        this.sourcecode = sourcecodeList;
        this.flags = ((isStateful ? 1 : 0) | (isMatch ? 2 : 0));
    }
    
    @Override
    public String toString() {
        return "ExecutionPlan(resultSetDesc:" + this.resultSetDesc + "paramsDesc:" + this.paramsDesc + "\noutputStreamKind:" + this.kindOfOutputStream + "\ndataSources:" + this.dataSources + "\naggObjFactories:" + this.aggObjFactories + "\nusedTypes:" + this.usedTypes + "\nhowToAggregate:" + this.howToAggregate + "\ntraceOptions:" + this.traceOptions + "\ndataset count:" + this.dataSetCount + "\nstateful:" + this.isStateful() + "\nisMatch:" + this.isMatch() + ")";
    }
    
    @JsonIgnore
    private static AggAlgo getAggAlgorithm(final boolean hasAggFunc, final boolean hasGroupBy) {
        final int val = (hasAggFunc ? 2 : 0) | (hasGroupBy ? 1 : 0);
        switch (val) {
            case 0: {
                return AggAlgo.SIMPLE;
            }
            case 1: {
                return AggAlgo.GROUPBY_ONLY;
            }
            case 2: {
                return AggAlgo.AGG_ONLY;
            }
            case 3: {
                return AggAlgo.AGG_AND_GROUP_BY;
            }
            default: {
                assert false;
                return AggAlgo.SIMPLE;
            }
        }
    }
    
    @JsonIgnore
    public List<UUID> getDataSources() {
        final ArrayList<UUID> ret = new ArrayList<UUID>();
        for (final DataSource ds : this.dataSources) {
            ret.add(ds.dataSourceID);
        }
        return ret;
    }
    
    public List<DataSource> getDataSourceList() {
        return this.dataSources;
    }
    
    public void setDataSourceList(final List<DataSource> dataSources) {
        this.dataSources = dataSources;
    }
    
    @JsonIgnore
    public DataSource findDataSource(final UUID id) {
        for (final DataSource ds : this.dataSources) {
            if (ds.dataSourceID.equals((Object)id)) {
                return ds;
            }
        }
        return null;
    }
    
    @JsonIgnore
    public Map<String, Long> makeFieldsMap() {
        long i = 1L;
        final Map<String, Long> map = new HashMap<String, Long>();
        for (final RSFieldDesc f : this.resultSetDesc) {
            map.put(f.name, i++);
        }
        return map;
    }
    
    public CQExecutionPlan clone() throws CloneNotSupportedException {
        return new CQExecutionPlan(this);
    }
    
    @JsonIgnore
    public boolean isStateful() {
        return (this.flags & 0x1) != 0x0;
    }
    
    @JsonIgnore
    public boolean isMatch() {
        return (this.flags & 0x2) != 0x0;
    }
    
    public static class DataSource implements Serializable, Cloneable
    {
        private static final long serialVersionUID = 181781829866062244L;
        public final String name;
        private UUID dataSourceID;
        public final UUID typeID;
        
        public DataSource() {
            this.name = null;
            this.dataSourceID = null;
            this.typeID = null;
        }
        
        public DataSource(final String name, final UUID dataSourceID, final UUID typeID) {
            this.name = name;
            this.dataSourceID = dataSourceID;
            this.typeID = typeID;
        }
        
        @Override
        public String toString() {
            return this.name + ":" + this.dataSourceID;
        }
        
        @JsonIgnore
        public UUID getDataSourceID() {
            return this.dataSourceID;
        }
        
        @JsonIgnore
        public void replaceDataSourceID(final UUID newid) {
            this.dataSourceID = newid;
        }
        
        public DataSource clone() throws CloneNotSupportedException {
            return (DataSource)super.clone();
        }
    }
    
    public interface CQExecutionPlanFactory
    {
        CQExecutionPlan createPlan() throws Exception;
    }
}
