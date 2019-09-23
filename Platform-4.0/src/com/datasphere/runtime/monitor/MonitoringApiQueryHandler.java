package com.datasphere.runtime.monitor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.RemoteCall;
import com.datasphere.runtime.DistributedExecutionManager;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;
import com.datasphere.hd.HDKey;

public class MonitoringApiQueryHandler implements RemoteCall<Object>
{
    private static final long serialVersionUID = -369249067395520309L;
    private static Logger logger;
    private final String storeName;
    private final HDKey key;
    private final String[] fields;
    private final Map<String, Object> filter;
    private final boolean getAllHD;
    AuthToken token;
    
    public MonitoringApiQueryHandler(final String storeName, final HDKey key, final String[] fields, final Map<String, Object> filter, final AuthToken token, final boolean getAllHD) {
        this.storeName = storeName;
        this.key = key;
        this.fields = fields.clone();
        this.filter = filter;
        this.token = token;
        this.getAllHD = getAllHD;
    }
    
    @Override
    public Object call() throws Exception {
        return this.get();
    }
    
    public Object getAllHDs() throws Exception {
        final MonitorModel monitorModel = MonitorModel.getInstance();
        if (monitorModel != null) {
            return monitorModel.getAllMonitorHDs(this.fields, this.filter, this.token);
        }
        return null;
    }
    
    public Object getHD() throws Exception {
        if (this.key.key != null && !(this.key.key instanceof UUID)) {
            this.key.key = new UUID(this.key.key.toString());
        }
        this.filter.put("cacheOnly", "true");
        final MonitorModel monitorModel = MonitorModel.getInstance();
        if (monitorModel != null) {
            return MonitorModel.getInstance().getMonitorHD(this.key, this.fields, this.filter);
        }
        return null;
    }
    
    public Object getMonitorDataRemote() throws MonitoringServerException {
        if (MonitoringApiQueryHandler.logger.isDebugEnabled()) {
            MonitoringApiQueryHandler.logger.debug((Object)"Monitoring server is running remotely");
        }
        final UUID MonitorModelServerId = (UUID)HazelcastSingleton.get().getMap("MonitorModelToServerMap").get((Object)"MonitorModel");
        if (MonitorModelServerId == null) {
            throw new MonitoringServerException("Monitoring server is not running");
        }
        if (MonitoringApiQueryHandler.logger.isDebugEnabled()) {
            MonitoringApiQueryHandler.logger.debug((Object)("Trying to get monitoring data from remote server " + MonitorModelServerId));
        }
        final Object tR = this.execute(this, MonitorModelServerId);
        Object results;
        if (tR instanceof Collection) {
            final Collection coll = (Collection)tR;
            if (coll.size() != 1) {
                throw new MonitoringServerException("Expected exactly 1 map(Map<HDKey, Map<String, Object>>) with All HDs. But got " + coll.size());
            }
            results = coll.iterator().next();
        }
        else {
            results = new HashMap();
        }
        return results;
    }
    
    private Object execute(final MonitoringApiQueryHandler action, final UUID serverId) throws MonitoringServerException {
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
        final Map<UUID, Member> valid = new HashMap<UUID, Member>();
        for (final Member member : srvs) {
            valid.put(new UUID(member.getUuid()), member);
        }
        final Set<Member> executeSrvs = new HashSet<Member>();
        Member member = valid.get(serverId);
        if (member != null) {
            executeSrvs.add(member);
            return DistributedExecutionManager.exec(hz, (Callable<Object>)action, executeSrvs);
        }
        throw new MonitoringServerException("Server " + serverId + " not found");
    }
    
    public Object get() throws Exception {
        MonitoringApiQueryHandler.logger.debug((Object)"Got Monitoring request");
        Object result;
        if (this.getAllHD) {
            result = this.getAllHDs();
        }
        else {
            result = this.getHD();
        }
        if (result == null) {
            result = this.getMonitorDataRemote();
        }
        if (MonitoringApiQueryHandler.logger.isDebugEnabled()) {
            MonitoringApiQueryHandler.logger.debug((Object)("Return result " + result));
        }
        return result;
    }
    
    static {
        MonitoringApiQueryHandler.logger = Logger.getLogger((Class)MonitoringApiQueryHandler.class);
    }
}
