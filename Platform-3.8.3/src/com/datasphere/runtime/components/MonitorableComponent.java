package com.datasphere.runtime.components;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.runtime.monitor.Monitorable;
import com.datasphere.uuid.UUID;

public abstract class MonitorableComponent implements Monitorable
{
    private static Logger logger;
    private Thread processThread;
    public Long prevTimeStamp;
    private Long prevCpuTime;
    
    public MonitorableComponent() {
        this.processThread = null;
        this.prevTimeStamp = null;
    }
    
    public abstract void addSpecificMonitorEvents(final MonitorEventsCollection p0);
    
    public abstract UUID getMetaID();
    
    public abstract BaseServer srv();
    
    public void setProcessThread() {
        if (this.processThread == null) {
            this.processThread = Thread.currentThread();
        }
    }
    
    public void resetProcessThread() {
        this.processThread = null;
    }
    
    public Thread getProcessThread() {
        return this.processThread;
    }
    
    @Override
    public Collection<MonitorEvent> getMonitorEvents(final long ts) {
        if (this.prevTimeStamp != null && this.prevTimeStamp == ts) {
            MonitorableComponent.logger.warn((Object)("Unexpected MonitorableComponent called for events with same previous timestamp: " + ts));
            return new ArrayList<MonitorEvent>();
        }
        final BaseServer srv = this.srv();
        if (srv != null) {
            final UUID serverID = srv.getServerID();
            final UUID entityID = this.getMetaID();
            final MonitorEventsCollection monEvs = new MonitorEventsCollection(ts, this.prevTimeStamp, serverID, entityID);
            this.addSpecificMonitorEvents(monEvs);
            if (this.processThread != null) {
                try {
                    final long cpuTime = ManagementFactory.getThreadMXBean().getThreadCpuTime(this.processThread.getId());
                    if (this.prevCpuTime != null) {
                        final long cpuDelta = Math.abs(cpuTime - this.prevCpuTime);
                        final long cpuRate = 1000L * cpuDelta / (ts - this.prevTimeStamp);
                        monEvs.add(MonitorEvent.Type.CPU_RATE, Long.valueOf(cpuRate));
                        monEvs.add(MonitorEvent.Type.CPU_THREAD, this.processThread.getName());
                    }
                    this.prevCpuTime = cpuTime;
                }
                catch (UnsupportedOperationException e) {
                    MonitorableComponent.logger.info((Object)"MXBean does not support CPU on this operating system", (Throwable)e);
                    monEvs.add(MonitorEvent.Type.CPU_RATE, Long.valueOf(-1L));
                }
                catch (Exception e2) {
                    MonitorableComponent.logger.error((Object)"Error getting thread CPU usage", (Throwable)e2);
                    monEvs.add(MonitorEvent.Type.CPU_RATE, Long.valueOf(-1L));
                }
            }
            this.prevTimeStamp = ts;
            return monEvs.getEvents();
        }
        return new ArrayList<MonitorEvent>();
    }
    
    static {
        MonitorableComponent.logger = Logger.getLogger((Class)MonitorableComponent.class);
    }
}
