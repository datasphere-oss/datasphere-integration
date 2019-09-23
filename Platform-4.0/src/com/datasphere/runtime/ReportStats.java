package com.datasphere.runtime;

import java.io.*;

import com.datasphere.event.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.hd.*;
import java.util.*;

public class ReportStats
{
    private static final long serialVersionUID = -1130287312659963240L;
    
    public static class BaseReportStats implements Serializable
    {
        private static final long serialVersionUID = -3438834212660192367L;
        public MetaInfo.MetaObject metaObj;
        
        public BaseReportStats() {
            this.metaObj = null;
        }
        
        public BaseReportStats(final MetaInfo.MetaObject obj) {
            this.metaObj = obj;
        }
        
        public MetaInfo.MetaObject getMetaObj() {
            return this.metaObj;
        }
        
        public void setMetaObj(final MetaInfo.MetaObject mObj) {
            this.metaObj = mObj;
        }
    }
    
    public static class SourceReportStats extends BaseReportStats
    {
        private static final long serialVersionUID = -3437234212660192367L;
        private long eventsSeen;
        private Event firstEvent;
        private String firstEventStr;
        private Event lastEvent;
        private String lastEventStr;
        private long firstEventTS;
        private long lastEventTS;
        
        public SourceReportStats() {
            this.eventsSeen = 0L;
            this.firstEvent = null;
            this.lastEvent = null;
            this.firstEventStr = null;
            this.lastEventStr = null;
        }
        
        public SourceReportStats(final MetaInfo.MetaObject obj) {
            super(obj);
            this.eventsSeen = 0L;
            this.firstEvent = null;
            this.lastEvent = null;
            this.firstEventStr = null;
            this.lastEventStr = null;
        }
        
        public long getEventsSeen() {
            return this.eventsSeen;
        }
        
        public void setEventsSeen(final long sInputNum) {
            this.eventsSeen = sInputNum;
        }
        
        public Event getFirstEvent() {
            return this.firstEvent;
        }
        
        public void setFirstEvent(final Event e) {
            this.firstEvent = e;
        }
        
        public Event getLastEvent() {
            return this.lastEvent;
        }
        
        public void setLastEvent(final Event e) {
            this.lastEvent = e;
        }
        
        public long getFirstEventTS() {
            return this.firstEventTS;
        }
        
        public void setFirstEventTS(final long eventTS) {
            this.firstEventTS = eventTS;
        }
        
        public long getLastEventTS() {
            return this.lastEventTS;
        }
        
        public void setLastEventTS(final long eventTS) {
            this.lastEventTS = eventTS;
        }
        
        public String getFirstEventStr() {
            return this.firstEventStr;
        }
        
        public void setFirstEventStr(final String str) {
            this.firstEventStr = str;
        }
        
        public String getLastEventStr() {
            return this.lastEventStr;
        }
        
        public void setLastEventStr(final String str) {
            this.lastEventStr = str;
        }
    }
    
    public static class HDstoreReportStats extends BaseReportStats
    {
        private static final long serialVersionUID = -3437234212660212367L;
        private long hdsSeen;
        private long lasthdTS;
        private HD lastHD;
        private String lastHDStr;
        
        public HDstoreReportStats() {
            this.hdsSeen = 0L;
            this.lastHD = null;
            this.lastHDStr = null;
            this.lasthdTS = 0L;
        }
        
        public HDstoreReportStats(final MetaInfo.MetaObject obj) {
            super(obj);
            this.hdsSeen = 0L;
            this.lastHD = null;
            this.lastHDStr = null;
            this.lasthdTS = 0L;
        }
        
        public long getHDsSeen() {
            return this.hdsSeen;
        }
        
        public void setHDsSeen(final long sInputNum) {
            this.hdsSeen = sInputNum;
        }
        
        public long getLasthdTS() {
            return this.lasthdTS;
        }
        
        public void setLasthdTS(final long hdts) {
            this.lasthdTS = hdts;
        }
        
        public HD getLastHD() {
            return this.lastHD;
        }
        
        public void setLastHD(final HD hd) {
            this.lastHD = hd;
        }
        
        public String getLastHDStr() {
            return this.lastHDStr;
        }
        
        public void setLastHDStr(final String hd) {
            this.lastHDStr = hd;
        }
    }
    
    public static class TargetReportStats extends BaseReportStats
    {
        private static final long serialVersionUID = -3437234212660212367L;
        private long eventsOutput;
        private long lastEventTS;
        private Event lastEvent;
        private String lastEventStr;
        
        public TargetReportStats() {
            this.eventsOutput = 0L;
            this.lastEventTS = 0L;
            this.lastEvent = null;
            this.lastEventStr = null;
        }
        
        public TargetReportStats(final MetaInfo.MetaObject obj) {
            super(obj);
            this.eventsOutput = 0L;
            this.lastEventTS = 0L;
            this.lastEvent = null;
            this.lastEventStr = null;
        }
        
        public long getEventsOutput() {
            return this.eventsOutput;
        }
        
        public void setEventsOutput(final long sInputNum) {
            this.eventsOutput = sInputNum;
        }
        
        public long getLastEventTS() {
            return this.lastEventTS;
        }
        
        public void setLastEventTS(final long eventTS) {
            this.lastEventTS = eventTS;
        }
        
        public Event getLastEvent() {
            return this.lastEvent;
        }
        
        public void setLastEvent(final Event e) {
            this.lastEvent = e;
        }
        
        public String getLastEventStr() {
            return this.lastEventStr;
        }
        
        public void setLastEventStr(final String evStr) {
            this.lastEventStr = evStr;
        }
    }
    
    public static class FlowReportStats extends BaseReportStats
    {
        private static final long serialVersionUID = -3437234212649192367L;
        public List<SourceReportStats> srcStats;
        public List<HDstoreReportStats> wsStats;
        public List<TargetReportStats> tgtStats;
        long checkpointTS;
        
        public FlowReportStats() {
            this.srcStats = new LinkedList<SourceReportStats>();
            this.wsStats = new LinkedList<HDstoreReportStats>();
            this.tgtStats = new LinkedList<TargetReportStats>();
            this.checkpointTS = -1L;
        }
        
        public FlowReportStats(final MetaInfo.MetaObject flobj) {
            super(flobj);
            this.srcStats = new LinkedList<SourceReportStats>();
            this.wsStats = new LinkedList<HDstoreReportStats>();
            this.tgtStats = new LinkedList<TargetReportStats>();
            this.checkpointTS = -1L;
        }
        
        public void setCheckpointTS(final long cptts) {
            this.checkpointTS = cptts;
        }
        
        public long getCheckpointTS() {
            return this.checkpointTS;
        }
    }
}
