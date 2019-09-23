package com.datasphere.persistence;

import com.datasphere.uuid.*;
import com.datasphere.runtime.*;
import java.util.concurrent.*;
import java.util.*;
import com.hazelcast.core.*;
import com.datasphere.metaRepository.*;
import com.datasphere.hd.*;

public class HStoreFinder
{
    public static Collection<Boolean> findAServerWhereWSIsDeployed(final String wsName, final AuthToken token) {
        Collection<Boolean> collection = null;
        final HazelcastInstance hz = HazelcastSingleton.get();
        final Set<Member> members = DistributedExecutionManager.getAllServersButMe(hz);
        if (members != null && !members.isEmpty()) {
            final IsHDStoreDeployed isDeployed = new IsHDStoreDeployed(wsName, token);
            collection = DistributedExecutionManager.exec(hz, (Callable<Boolean>)isDeployed, members);
            if (collection != null) {
                collection.removeAll(Collections.singleton(false));
            }
        }
        return collection;
    }
    
    public static class IsHDStoreDeployed implements RemoteCall<Boolean>
    {
        String wsName;
        AuthToken token;
        
        public IsHDStoreDeployed(final String wsName, final AuthToken authToken) {
            this.wsName = null;
            this.token = null;
            this.wsName = wsName;
            this.token = authToken;
        }
        
        @Override
        public Boolean call() throws Exception {
            if (HDApiQueryHandler.canFetchHDs(this.wsName, this.token)) {
                return new Boolean(true);
            }
            return new Boolean(false);
        }
    }
}
