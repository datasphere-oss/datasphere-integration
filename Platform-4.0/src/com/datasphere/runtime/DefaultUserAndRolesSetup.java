package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.exception.SecurityException;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.ObjectPermission;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.AuthToken;

public class DefaultUserAndRolesSetup
{
    private static Logger logger;
    private static MDRepository metadataRepository;
    private static HSecurityManager security_manager;
    private static AuthToken sessionID;
    
    public static void Setup(final HSecurityManager security_manager) throws Exception {
        DefaultUserAndRolesSetup.metadataRepository = MetadataRepository.getINSTANCE();
        DefaultUserAndRolesSetup.security_manager = security_manager;
        createAdminUserAndRoleSetup();
    }
    
    private static void createAdminUserAndRoleSetup() throws Exception {
        if (DefaultUserAndRolesSetup.security_manager.getUser("admin") == null) {
            final String adminPassword = System.getProperty("com.datasphere.config.adminPassword", System.getenv("HD_ADMIN_PASSWORD"));
            String password = null;
            boolean verified = false;
            if (adminPassword != null && !adminPassword.isEmpty()) {
                if (DefaultUserAndRolesSetup.logger.isInfoEnabled()) {
                    DefaultUserAndRolesSetup.logger.info((Object)("Using password sent through system properties : " + adminPassword));
                }
                password = adminPassword;
                verified = true;
            }
            else {
                printf("Required property \"Admin Password\" is undefined\n");
                password = ConsoleReader.readPassword("Enter Admin Password: ");
                for (int count = 0; count < 3; ++count) {
                    final String passToString2 = ConsoleReader.readPassword("Re-enter the password : ");
                    if (password.equals(passToString2)) {
                        if (DefaultUserAndRolesSetup.logger.isInfoEnabled()) {
                            DefaultUserAndRolesSetup.logger.info((Object)"Matched Password");
                        }
                        verified = true;
                        break;
                    }
                    DefaultUserAndRolesSetup.logger.error((Object)"Password did not match");
                    verified = false;
                }
            }
            if (verified) {
                createAdmin(password);
                createDefaultRoles();
                if (DefaultUserAndRolesSetup.logger.isInfoEnabled()) {
                    DefaultUserAndRolesSetup.logger.info((Object)"done creating default roles - operator, appadmin, appdev, appuser");
                }
            }
            else {
                System.exit(1);
            }
        }
    }
    
    private static void createAdmin(final String password) throws Exception {
        if (DefaultUserAndRolesSetup.metadataRepository.getMetaObjectByName(EntityType.ROLE, "Global", "admin", null, DefaultUserAndRolesSetup.sessionID) == null) {
            final MetaInfo.Role newRole = new MetaInfo.Role();
            newRole.construct(MetaInfo.GlobalNamespace, "admin");
            newRole.grantPermission(new ObjectPermission("*"));
            DefaultUserAndRolesSetup.metadataRepository.putMetaObject(newRole, DefaultUserAndRolesSetup.sessionID);
        }
        if (DefaultUserAndRolesSetup.security_manager.getUser("admin") == null) {
            MetaInfo.User newuser = null;
            final MetaInfo.Role r = (MetaInfo.Role)DefaultUserAndRolesSetup.metadataRepository.getMetaObjectByName(EntityType.ROLE, "Global", "admin", null, DefaultUserAndRolesSetup.sessionID);
            final List<String> lrole = new ArrayList<String>();
            lrole.add(r.getRole());
            newuser = new MetaInfo.User();
            newuser.construct("admin", password);
            DefaultUserAndRolesSetup.security_manager.addUser(newuser, DefaultUserAndRolesSetup.sessionID);
            newuser = DefaultUserAndRolesSetup.security_manager.getUser("admin");
            DefaultUserAndRolesSetup.security_manager.grantUserRoles(newuser.getUserId(), lrole, DefaultUserAndRolesSetup.sessionID);
            newuser = DefaultUserAndRolesSetup.security_manager.getUser("admin");
        }
    }
    
    private static void createDefaultRoles() throws Exception {
        if (DefaultUserAndRolesSetup.security_manager.getRole("Global:appadmin") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "appadmin");
            role.grantPermissions(getDefaultAppAdminPermissions());
            DefaultUserAndRolesSetup.security_manager.addRole(role, DefaultUserAndRolesSetup.sessionID);
        }
        if (DefaultUserAndRolesSetup.security_manager.getRole("Global:appdev") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "appdev");
            role.grantPermissions(getDefaultDevPermissions());
            DefaultUserAndRolesSetup.security_manager.addRole(role, DefaultUserAndRolesSetup.sessionID);
        }
        if (DefaultUserAndRolesSetup.security_manager.getRole("Global:appuser") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "appuser");
            role.grantPermissions(getDefaultEndUserPermissions());
            DefaultUserAndRolesSetup.security_manager.addRole(role, DefaultUserAndRolesSetup.sessionID);
        }
        if (DefaultUserAndRolesSetup.security_manager.getRole("Global:systemuser") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "systemuser");
            role.grantPermissions(getDefaultSystemUserPermissions());
            DefaultUserAndRolesSetup.security_manager.addRole(role, DefaultUserAndRolesSetup.sessionID);
        }
        if (DefaultUserAndRolesSetup.security_manager.getRole("Global:uiuser") == null) {
            final MetaInfo.Role role = new MetaInfo.Role();
            role.construct(MetaInfo.GlobalNamespace, "uiuser");
            role.grantPermissions(getDefaultUIUserPermissions());
            DefaultUserAndRolesSetup.security_manager.addRole(role, DefaultUserAndRolesSetup.sessionID);
        }
    }
    
    private static List<ObjectPermission> getDefaultSystemUserPermissions() throws Exception {
        final List<ObjectPermission> defaultUserPermissions = new ArrayList<ObjectPermission>();
        final Set<String> domains = new HashSet<String>();
        final Set<ObjectPermission.Action> actions = new HashSet<ObjectPermission.Action>();
        final Set<ObjectPermission.ObjectType> objectTypes = new HashSet<ObjectPermission.ObjectType>();
        final Set<String> names = new HashSet<String>();
        final Set<String> domains2 = new HashSet<String>();
        final Set<ObjectPermission.Action> actions2 = new HashSet<ObjectPermission.Action>();
        final Set<ObjectPermission.ObjectType> objectTypes2 = new HashSet<ObjectPermission.ObjectType>();
        domains2.add("Global");
        actions2.add(ObjectPermission.Action.select);
        actions2.add(ObjectPermission.Action.read);
        objectTypes2.add(ObjectPermission.ObjectType.propertytemplate);
        objectTypes2.add(ObjectPermission.ObjectType.deploymentgroup);
        objectTypes2.add(ObjectPermission.ObjectType.type);
        final ObjectPermission readPropTemplatesPerm = new ObjectPermission(domains2, actions2, objectTypes2, null);
        defaultUserPermissions.add(readPropTemplatesPerm);
        return defaultUserPermissions;
    }
    
    private static List<ObjectPermission> getDefaultUIUserPermissions() throws Exception {
        final List<ObjectPermission> permissions = new ArrayList<ObjectPermission>();
        final ObjectPermission p1 = new ObjectPermission("*:*:dashboard_ui,apps_ui,sourcepreview_ui,monitor_ui:*");
        permissions.add(p1);
        return permissions;
    }
    
    private static List<ObjectPermission> getDefaultAppAdminPermissions() throws Exception {
        final List<ObjectPermission> permissions = new ArrayList<ObjectPermission>();
        final ObjectPermission p1 = new ObjectPermission("Global:create:namespace:*");
        permissions.add(p1);
        final ObjectPermission p2 = new ObjectPermission("Global:read,select:*:*");
        permissions.add(p2);
        return permissions;
    }
    
    private static List<ObjectPermission> getDefaultDevPermissions() throws SecurityException {
        final List<ObjectPermission> permissions = new ArrayList<ObjectPermission>();
        final ObjectPermission p1 = new ObjectPermission("Global:namespace:*");
        permissions.add(p1);
        final ObjectPermission p2 = new ObjectPermission("Global:read,select:*:*");
        permissions.add(p2);
        return permissions;
    }
    
    private static List<ObjectPermission> getDefaultEndUserPermissions() throws SecurityException {
        final List<ObjectPermission> permissions = new ArrayList<ObjectPermission>();
        final ObjectPermission p2 = new ObjectPermission("Global:read,select:*:*");
        permissions.add(p2);
        return permissions;
    }
    
    private static void printf(final String toPrint) {
        if (System.console() != null) {
            System.console().printf(toPrint, new Object[0]);
        }
        else {
            System.out.print(toPrint);
        }
        System.out.flush();
    }
    
    public static void main(final String[] args) {
    }
    
    static {
        DefaultUserAndRolesSetup.logger = Logger.getLogger((Class)DefaultUserAndRolesSetup.class);
        DefaultUserAndRolesSetup.sessionID = HSecurityManager.TOKEN;
    }
}
