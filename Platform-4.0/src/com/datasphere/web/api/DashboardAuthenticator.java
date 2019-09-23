package com.datasphere.web.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.QueryValidator;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class DashboardAuthenticator
{
    private static Logger logger;
    private QueryValidator qv;
    private HSecurityManager securityManager;
    
    public DashboardAuthenticator(final QueryValidator qv) {
        this.qv = qv;
        this.securityManager = HSecurityManager.get();
    }
    
    public AuthToken authenticateDashboardEmbedUser(final String token) throws Exception {
        final String[] chunks = token.split(":");
        if (chunks.length != 2) {
            throw new SecurityException("Invalid Token provided  (1) ");
        }
        final String username = chunks[0];
        final String key = chunks[1];
        final MetaInfo.User dashboardEmbedUser = (MetaInfo.User)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.USER, "Global", username, -1, HSecurityManager.TOKEN);
        if (dashboardEmbedUser == null) {
            throw new MetaDataRepositoryException("Authentication failed (1)");
        }
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final String passInDB = dashboardEmbedUser.getUuid().getUUIDString();
        final byte[] hash = digest.digest(passInDB.getBytes(StandardCharsets.UTF_8));
        final byte[] encodedHash = Base64.getEncoder().encode(hash);
        final String keyFromDB = new String(encodedHash);
        if (keyFromDB.equals(key)) {
            return this.securityManager.authenticate(username, dashboardEmbedUser.uuid.getUUIDString());
        }
        throw new Exception("Invalid Token provided (2) ");
    }
    
    public Set<String> getPermissionsForDashboard(final AuthToken token, final String dashboardFQN) throws Exception {
        final Set<String> permissions = new HashSet<String>();
        final String nsName = Utility.splitDomain(dashboardFQN);
        final String name = Utility.splitName(dashboardFQN);
        final MetaInfo.Dashboard dashboard = (MetaInfo.Dashboard)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.DASHBOARD, nsName, name, -1, token);
        if (dashboard == null) {
            throw new MetaDataRepositoryException("Could not find dashboard with id: " + dashboardFQN);
        }
        permissions.add("GLOBAL:READ:NAMESPACE:" + nsName);
        permissions.add("GLOBAL:READ:DEPLOYMENTGROUP:*");
        permissions.add(dashboard.getNsName() + ":READ:DASHBOARD:" + dashboard.getName());
        final List<MetaInfo.Page> pages = new ArrayList<MetaInfo.Page>();
        for (String pageName : dashboard.getPages()) {
            String pageNSName = nsName;
            if (pageName.indexOf(".") != -1) {
                pageNSName = Utility.splitDomain(pageName);
            }
            pageName = Utility.splitName(pageName);
            final MetaInfo.Page page = (MetaInfo.Page)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PAGE, pageNSName, pageName, -1, token);
            if (page != null) {
                pages.add(page);
                permissions.add(page.getNsName() + ":READ:PAGE:" + page.getName());
            }
        }
        final List<MetaInfo.QueryVisualization> queryVisualizations = new ArrayList<MetaInfo.QueryVisualization>();
        for (final MetaInfo.Page page2 : pages) {
            for (String queryVisualizationName : page2.getQueryVisualizations()) {
                String queryVisualizationNSName = nsName;
                if (queryVisualizationName.indexOf(".") != -1) {
                    queryVisualizationNSName = Utility.splitDomain(queryVisualizationName);
                }
                queryVisualizationName = Utility.splitName(queryVisualizationName);
                final MetaInfo.QueryVisualization queryVisualization = (MetaInfo.QueryVisualization)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.QUERYVISUALIZATION, queryVisualizationNSName, queryVisualizationName, -1, token);
                if (queryVisualization != null) {
                    queryVisualizations.add(queryVisualization);
                    permissions.add(queryVisualization.getNsName() + ":READ:QUERYVISUALIZATION:" + queryVisualization.getName());
                }
            }
        }
        final List<MetaInfo.Query> queries = new ArrayList<MetaInfo.Query>();
        for (final MetaInfo.QueryVisualization queryVisualization2 : queryVisualizations) {
            String queryNSName = nsName;
            String queryName = queryVisualization2.getQuery();
            if (queryVisualization2.getQuery() != null) {
                if (queryVisualization2.getQuery().isEmpty()) {
                    continue;
                }
                if (queryVisualization2.getQuery().indexOf(".") != -1) {
                    queryNSName = Utility.splitDomain(queryName);
                }
                queryName = Utility.splitName(queryName);
                final MetaInfo.Query query = (MetaInfo.Query)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.QUERY, queryNSName, queryName, -1, token);
                if (query == null) {
                    continue;
                }
                queries.add(query);
                permissions.add(query.getNsName() + ":SELECT,READ:QUERY:" + query.getName());
                final MetaInfo.Flow app = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(query.appUUID, token);
                if (app == null) {
                    continue;
                }
                final Set<UUID> components = app.getAllObjects();
                for (final UUID componentUUID : components) {
                    final MetaInfo.MetaObject component = MetadataRepository.getINSTANCE().getMetaObjectByUUID(componentUUID, token);
                    if (!EntityType.WASTOREVIEW.equals(component.type)) {
                        permissions.add(component.nsName + ":SELECT,READ:" + component.type + ":" + component.name);
                    }
                }
                final Set<UUID> wastoreviews = app.getObjects().get(EntityType.WASTOREVIEW);
                if (wastoreviews == null) {
                    continue;
                }
                final UUID wastoreviewUUID = wastoreviews.iterator().next();
                final MetaInfo.WAStoreView wastoreview = (MetaInfo.WAStoreView)MetadataRepository.getINSTANCE().getMetaObjectByUUID(wastoreviewUUID, token);
                if (wastoreview == null) {
                    continue;
                }
                final UUID hdStoreUUID = wastoreview.wastoreID;
                final MetaInfo.HDStore hdStore = (MetaInfo.HDStore)MetadataRepository.getINSTANCE().getMetaObjectByUUID(hdStoreUUID, token);
                permissions.add(hdStore.nsName + ":SELECT,READ:HDSTORE:" + hdStore.name);
                permissions.add("GLOBAL:READ:NAMESPACE:" + hdStore.getNsName());
            }
        }
        permissions.add("*:*:DASHBOARD_UI:*");
        permissions.add(nsName + ":READ:APPLICATION:*");
        permissions.add(nsName + ":READ:CQ:*");
        permissions.add(nsName + ":READ:QUERY:*");
        return permissions;
    }
    
    public String createDashboardEmbedUser(final AuthToken token, final String dashboardFQN) throws Exception {
        final String nsName = Utility.splitDomain(dashboardFQN);
        final String name = Utility.splitName(dashboardFQN);
        final MetaInfo.Dashboard dashboard = (MetaInfo.Dashboard)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.DASHBOARD, nsName, name, -1, token);
        if (dashboard == null) {
            throw new MetaDataRepositoryException("Could not find dashboard with id: " + dashboardFQN);
        }
        final Set<String> permissions = this.getPermissionsForDashboard(token, dashboardFQN);
        final String userID = nsName + "" + name + "Embed";
        MetaInfo.User dashboardEmbedUser = (MetaInfo.User)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.USER, "Global", userID, -1, token);
        if (dashboardEmbedUser == null) {
            dashboardEmbedUser = new MetaInfo.User();
            dashboardEmbedUser.construct(userID, "TEMPPASSWORD");
            this.securityManager.addUser(dashboardEmbedUser, token);
            this.securityManager.setPassword(dashboardEmbedUser.getUserId(), dashboardEmbedUser.getUuid().getUUIDString(), "TEMPPASSWORD", token);
            dashboardEmbedUser = (MetaInfo.User)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.USER, "Global", userID, -1, token);
        }
        permissions.add("GLOBAL:READ:USER:" + userID);
        final List<String> permissionsList = new ArrayList<String>(permissions);
        this.securityManager.grantUserPermissions(userID, permissionsList, token);
        String authToken = dashboardEmbedUser.getUserId().toUpperCase() + ":";
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final byte[] hash = digest.digest(dashboardEmbedUser.getUuid().getUUIDString().getBytes(StandardCharsets.UTF_8));
        final byte[] encodedHash = Base64.getEncoder().encode(hash);
        final String hashString = new String(encodedHash);
        authToken += hashString;
        return authToken;
    }
    
    static {
        DashboardAuthenticator.logger = Logger.getLogger((Class)DashboardAuthenticator.class);
    }
}
