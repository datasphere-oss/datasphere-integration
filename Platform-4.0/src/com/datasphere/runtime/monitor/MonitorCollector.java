package com.datasphere.runtime.monitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.datasphere.anno.AdapterType;
import com.datasphere.anno.PropertyTemplate;
import com.datasphere.event.Event;
import com.datasphere.common.exc.SystemException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.SourceProcess;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.components.Flow;
import com.datasphere.runtime.components.FlowComponent;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

@PropertyTemplate(name = "MonitoringDataSource", type = AdapterType.internal, properties = {}, inputType = MonitorEvent.class, outputType = MonitorBatchEvent.class)
public class MonitorCollector extends SourceProcess
{
    private static Logger logger;
    private static final long PERIOD_MILLIS = 5000L;
    private BaseServer srv;
    private static Queue<MonitorEvent> monEventQueue;
    private double maxAllowedHeapUsagePercent;
    
    public MonitorCollector() {
        this.srv = null;
    }
    
    public void collectEvents() throws InterruptedException {
        final long ts = System.currentTimeMillis();
        try {
            MonitorCollector.logger.debug((Object)"Collecting monitoring events");
            final List<MonitorEvent> allEvents = new ArrayList<MonitorEvent>();
            final Collection<Channel> channels = this.srv.getAllChannelsView();
            for (final Channel channel : channels) {
                final Collection<MonitorEvent> monitorEvents = channel.getMonitorEvents(ts);
                if (monitorEvents != null) {
                    allEvents.addAll(monitorEvents);
                }
            }
            final Collection<FlowComponent> allObjects = this.srv.getAllObjectsView();
            for (final FlowComponent theObject : allObjects) {
                final Collection<MonitorEvent> monitorEvents2 = theObject.getMonitorEvents(ts);
                if (monitorEvents2 != null) {
                    allEvents.addAll(monitorEvents2);
                }
                if (theObject.getMetaInfo().type.equals(EntityType.FLOW) || theObject.getMetaInfo().type.equals(EntityType.APPLICATION)) {
                    final Collection<MonitorEvent> streamMonEvents = ((Flow)theObject).getStreamMonEvs(ts);
                    if (streamMonEvents == null) {
                        continue;
                    }
                    allEvents.addAll(streamMonEvents);
                }
            }
            final Collection<MonitorEvent> monitorEvents3 = this.srv.getMonitorEvents(ts);
            if (monitorEvents3 != null) {
                long freeMemory = -1L;
                long maxMemory = -1L;
                final Iterator<MonitorEvent> iter = monitorEvents3.iterator();
                for (final MonitorEvent mEvent : monitorEvents3) {
                    if (mEvent.type == MonitorEvent.Type.MEMORY_FREE) {
                        freeMemory = mEvent.valueLong;
                    }
                    else if (mEvent.type == MonitorEvent.Type.MEMORY_MAX) {
                        maxMemory = mEvent.valueLong;
                    }
                    allEvents.add(mEvent);
                }
                if (freeMemory != -1L && maxMemory != -1L && (maxMemory - freeMemory) / maxMemory > this.maxAllowedHeapUsagePercent / 100.0) {
                    final Collection<FlowComponent> flComps = Server.server.getAllObjectsView();
                    for (final FlowComponent comp : flComps) {
                        if (comp.getMetaType() == EntityType.APPLICATION && !comp.getMetaNsName().equals("Global")) {
                            final MetaInfo.StatusInfo statusInfo = MetadataRepository.getINSTANCE().getStatusInfo(comp.getMetaID(), HSecurityManager.TOKEN);
                            if (statusInfo.getStatus() != MetaInfo.StatusInfo.Status.RUNNING) {
                                continue;
                            }
                            MonitorCollector.logger.warn((Object)("Crashing component " + comp.getMetaFullName() + " due to not enough memory. Freememory - " + freeMemory + ", MaxMemory - " + maxMemory + ", AllowedHeapPercent - " + this.maxAllowedHeapUsagePercent));
                            comp.notifyAppMgr(comp.getMetaType(), comp.getMetaFullName(), comp.getMetaID(), (Exception)new SystemException("Server " + BaseServer.getServerName() + " exceeded heap usage threshold of " + this.maxAllowedHeapUsagePercent), null, (Object[])null);
                        }
                    }
                }
            }
            for (MonitorEvent monEvent = MonitorCollector.monEventQueue.poll(); monEvent != null; monEvent = MonitorCollector.monEventQueue.poll()) {
                allEvents.add(monEvent);
            }
            final MonitorBatchEvent batchEvent = new MonitorBatchEvent(System.currentTimeMillis(), allEvents);
            this.send((Event)batchEvent, 0);
        }
        catch (Throwable e) {
            MonitorCollector.logger.error((Object)"Problem running Monitor", e);
        }
        try {
            Thread.sleep(5000L);
        }
        catch (InterruptedException ex) {}
    }
    
    @Override
    public synchronized void init(final Map<String, Object> properties, final Map<String, Object> parserProperties, final UUID uuid, final String distributionID) throws Exception {
        super.init(properties, parserProperties, uuid, distributionID);
        this.srv = BaseServer.getBaseServer();
        try {
            this.maxAllowedHeapUsagePercent = Double.parseDouble(System.getProperty("com.datasphere.maxAllowedHeapUsagePercent", "90"));
            if (this.maxAllowedHeapUsagePercent > 100.0) {
                this.maxAllowedHeapUsagePercent = 90.0;
            }
        }
        catch (Exception e) {
            MonitorCollector.logger.warn((Object)"Invalid setting for maxAllowedHeapUsagePercent, defaulting to 90");
            this.maxAllowedHeapUsagePercent = 90.0;
        }
    }
    
    @Override
    public void receiveImpl(final int channel, final Event event) throws Exception {
        this.collectEvents();
    }
    
    @Override
    public void close() throws Exception {
        super.close();
    }
    
    public static void reportIssue(final UUID objectID, final String message) {
        final MonitorEvent monEvent = new MonitorEvent(Server.server.getServerID(), objectID, MonitorEvent.Type.LOG_ERROR, message, Long.valueOf(System.currentTimeMillis()));
        MonitorCollector.monEventQueue.add(monEvent);
    }
    
    public static void reportStateChange(final UUID objectID, final String statusChangeString) {
        final MonitorEvent monEvent = new MonitorEvent(BaseServer.getBaseServer().getServerID(), objectID, MonitorEvent.Type.STATUS_CHANGE, statusChangeString, Long.valueOf(System.currentTimeMillis()));
        MonitorCollector.monEventQueue.add(monEvent);
    }
    
    public static void reportMonitorEvent(final MonitorEvent monEvent) {
        MonitorCollector.monEventQueue.add(monEvent);
    }
    
    static {
        MonitorCollector.logger = Logger.getLogger((Class)MonitorCollector.class);
        MonitorCollector.monEventQueue = new ConcurrentLinkedQueue<MonitorEvent>();
    }
}
