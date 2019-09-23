package com.datasphere.hd;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.runtime.*;
import com.datasphere.persistence.*;
import com.datasphere.runtime.containers.*;
import java.util.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.components.*;
import com.datasphere.utility.*;
import com.datasphere.security.*;
import com.datasphere.exception.*;
import com.datasphere.metaRepository.*;

public class HDApiQueryHandler implements RemoteCall<Object>
{
    private static final long serialVersionUID = -369249067395520309L;
    private static Logger logger;
    private final String storeName;
    private final HDKey key;
    private final String[] fields;
    private final Map<String, Object> filter;
    private final String queryString;
    private final Map<String, Object> queryParams;
    AuthToken token;
    
    public HDApiQueryHandler(final String storename, final String queryString, final Map<String, Object> queryParams, final AuthToken token) {
        this.storeName = storename;
        this.key = null;
        this.fields = null;
        this.filter = null;
        this.token = token;
        this.queryParams = queryParams;
        this.queryString = queryString;
    }
    
    public HDApiQueryHandler(final String storeName, final HDKey key, final String[] fields, final Map<String, Object> filter, final AuthToken token) {
        this.storeName = storeName;
        this.key = key;
        this.fields = fields.clone();
        this.filter = filter;
        this.token = token;
        this.queryParams = null;
        this.queryString = null;
    }
    
    @Override
    public Object call() throws Exception {
        Object mapMapMap = null;
        if (this.queryString != null) {
            final QueryRunnerForHDApi runner = new QueryRunnerForHDApi();
            final QueryValidator qVal = new QueryValidator(Server.server);
            runner.setQueryString(this.queryString, this.queryParams, qVal, this.token);
            final ITaskEvent tE = runner.call();
            runner.cleanUp();
            if (HDApiQueryHandler.logger.isDebugEnabled()) {
                HDApiQueryHandler.logger.debug((Object)("Size of result set as seem at WAPI using Adhoc infrastructure:" + tE.batch().size()));
                HDApiQueryHandler.logger.debug((Object)("Result Set as seen at WAPI using Adhoc infrastructure:" + tE));
            }
            final HStore ws = HStore.get(this.storeName);
            final String keyField = ws.contextBeanDef.keyFields.get(0);
            final Map<String, String> allFields = ws.contextBeanDef.fields;
            if (tE != null && keyField != null) {
                mapMapMap = HDApi.covertTaskEventsToMap(tE, keyField, this.fields, ws.contextBeanDef.fields, true);
            }
            if (HDApiQueryHandler.logger.isDebugEnabled()) {
                HDApiQueryHandler.logger.debug((Object)("Size of result set as seem at WAPI using old Query API:" + tE.batch().size()));
                HDApiQueryHandler.logger.debug((Object)("Result of getHDs from HD Store using old Query API: " + tE));
            }
            return mapMapMap;
        }
        if (!canFetchHDs(this.storeName, this.token)) {
            return null;
        }
        final HStore ws2 = HStore.get(this.storeName);
        if (ws2.usesNewHDStore()) {
            final QueryRunnerForHDApi runner = new QueryRunnerForHDApi();
            final QueryValidator qVal = new QueryValidator(Server.server);
            final String keyField2 = ws2.contextBeanDef.keyFields.get(0);
            final Map<String, String> allFields2 = ws2.contextBeanDef.fields;
            if (this.fields == null || this.fields[0].equalsIgnoreCase("default")) {
                final String[] implicitlyAddedFields = this.addImplicitFields(allFields2);
                runner.setAllFields(this.storeName, implicitlyAddedFields, this.filter, this.token, keyField2, false, qVal);
            }
            else if (this.fields[0].equalsIgnoreCase("default-allEvents")) {
                final String[] implicitlyAddedFields = this.addImplicitFields(allFields2);
                runner.setAllFields(this.storeName, implicitlyAddedFields, this.filter, this.token, keyField2, true, qVal);
            }
            else if (this.fields[0].equalsIgnoreCase("eventList")) {
                runner.setAllFields(this.storeName, null, this.filter, this.token, null, true, qVal);
            }
            else {
                runner.setAllFields(this.storeName, this.fields, this.filter, this.token, keyField2, false, qVal);
            }
            final ITaskEvent tE2 = runner.call();
            runner.cleanUp();
            if (HDApiQueryHandler.logger.isDebugEnabled()) {
                HDApiQueryHandler.logger.debug((Object)("Size of result set as seem at WAPI using Adhoc infrastructure:" + tE2.batch().size()));
                HDApiQueryHandler.logger.debug((Object)("Result Set as seen at WAPI using Adhoc infrastructure:" + tE2));
            }
            if (tE2 != null && keyField2 != null) {
                mapMapMap = HDApi.covertTaskEventsToMap(tE2, keyField2, this.fields, ws2.contextBeanDef.fields, false);
            }
            if (HDApiQueryHandler.logger.isDebugEnabled()) {
                HDApiQueryHandler.logger.debug((Object)("Size of result set as seem at WAPI using old Query API:" + tE2.batch().size()));
                HDApiQueryHandler.logger.debug((Object)("Result of getHDs from HD Store using old Query API: " + tE2));
            }
            return mapMapMap;
        }
        mapMapMap = ws2.getHDs(this.key, this.fields, this.filter);
        return mapMapMap;
    }
    
    public String[] addImplicitFields(final Map<String, String> allFields) {
        final List<String> implicitlyAddedFields = new ArrayList<String>();
        final Iterator<String> fieldSet = allFields.keySet().iterator();
        while (fieldSet.hasNext()) {
            implicitlyAddedFields.add(fieldSet.next());
        }
        final String[] converted = new String[implicitlyAddedFields.size()];
        return implicitlyAddedFields.toArray(converted);
    }
    
    public static boolean canFetchHDs(final String storename, final AuthToken token) throws NullPointerException, MetaDataRepositoryException {
        boolean isDeployed = false;
        final String[] splitNames = storename.split("\\.");
        final String nsName = (splitNames.length == 2) ? splitNames[0] : null;
        final String objName = (splitNames.length == 2) ? splitNames[1] : splitNames[0];
        final MetaInfo.HDStore hdStoreInfo = (MetaInfo.HDStore)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.HDSTORE, nsName, objName, null, token);
        if (hdStoreInfo == null) {
            throw new NullPointerException("No HD Store: " + storename + " found");
        }
        final ObjectPermission permission = new ObjectPermission(hdStoreInfo.getNsName(), ObjectPermission.Action.read, Utility.getPermissionObjectType(hdStoreInfo.getType()), hdStoreInfo.getName(), ObjectPermission.PermissionType.allow);
        final boolean isAllowed = HSecurityManager.get().isAllowed(token, permission);
        if (isAllowed) {
            try {
                if (Server.server != null) {
                    Server.server.checkNotDeployed(hdStoreInfo);
                }
            }
            catch (ServerException e) {
                isDeployed = true;
            }
        }
        return isDeployed;
    }
    
    static {
        HDApiQueryHandler.logger = Logger.getLogger((Class)HDApiQueryHandler.class);
    }
}
