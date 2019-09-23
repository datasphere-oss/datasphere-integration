package com.datasphere.security;

import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.datasphere.exception.InvalidTokenException;
import com.datasphere.exception.SecurityException;
import com.datasphere.intf.AuthLayer;
import com.hazelcast.core.IMap;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class HSecurityManager
{
    private static Logger logger;
    private static HSecurityManager instance;
    private static final char[] PASSWORD;
    private static boolean FIRSTTIME;
    public static final AuthToken TOKEN;
    public static final UUID ENCRYPTION_SALT;
    private final MDRepository metadataRepository;
    private static Map<String, MetaInfo.User> userMap;
    private static final String cipher = "PBEWithMD5AndDES";
    private static final String cipher_padded = "PBEWithMD5AndDES/CBC/PKCS5Padding";
    
    public static HSecurityManager get() {
        if (HSecurityManager.instance == null) {
            HSecurityManager.instance = new HSecurityManager();
        }
        return HSecurityManager.instance;
    }
    
    public static void shutdown() {
        HSecurityManager.instance = null;
    }
    
    private HSecurityManager() {
        if (HSecurityManager.logger.isInfoEnabled()) {
            HSecurityManager.logger.info("creating security manager");
        }
        this.metadataRepository = MetadataRepository.getINSTANCE();
        HSecurityManager.userMap = new HashMap<String, MetaInfo.User>();
    }
    
    public static void setFirstTime(final boolean firsttime) throws SecurityException {
        if (firsttime) {
            throw new SecurityException("you can not set this to true.");
        }
        HSecurityManager.FIRSTTIME = firsttime;
    }
    
    public static String encrypt(final String property, final byte[] salt) throws GeneralSecurityException, UnsupportedEncodingException {
        if (property == null) {
            return null;
        }
        try {
            return encryptInternal(property, salt, "PBEWithMD5AndDES");
        }
        catch (IllegalBlockSizeException ibe) {
            try {
                HSecurityManager.logger.warn(("encryption failed with IllegalBlockSizeException. So using padded cipher. Length of prop :" + property.length()));
                return encryptInternal(property, salt, "PBEWithMD5AndDES/CBC/PKCS5Padding");
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    private static String encryptInternal(final String property, final byte[] salt, final String cp) throws GeneralSecurityException, UnsupportedEncodingException {
        final SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        final SecretKey key = keyFactory.generateSecret(new PBEKeySpec(HSecurityManager.PASSWORD));
        final Cipher pbeCipher = Cipher.getInstance("PBEWithMD5AndDES");
        pbeCipher.init(1, key, new PBEParameterSpec(salt, 20));
        return base64Encode(pbeCipher.doFinal(property.getBytes("UTF-8")));
    }
    
    private static String base64Encode(final byte[] bytes) {
        return Base64.encodeBase64String(bytes);
    }
    
    public static String decrypt(final String encryptedProp, final byte[] salt) throws Exception {
        if (encryptedProp == null) {
            return null;
        }
        try {
            return decryptInternal(encryptedProp, salt, "PBEWithMD5AndDES");
        }
        catch (IllegalBlockSizeException ibe) {
            try {
                HSecurityManager.logger.warn(("decryption failed with IllegalBlockSizeException. So using padded cipher. Length of encryptedProp :" + encryptedProp.length()));
                return decryptInternal(encryptedProp, salt, "PBEWithMD5AndDES/CBC/PKCS5Padding");
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    private static String decryptInternal(final String encryptedProp, final byte[] salt, final String cp) throws Exception {
        final SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        final SecretKey key = keyFactory.generateSecret(new PBEKeySpec(HSecurityManager.PASSWORD));
        final Cipher pbeCipher = Cipher.getInstance(cp);
        pbeCipher.init(2, key, new PBEParameterSpec(salt, 20));
        final byte[] dec = Base64.decodeBase64(encryptedProp.getBytes("UTF8"));
        final byte[] utf8 = pbeCipher.doFinal(dec);
        return new String(utf8, "UTF8");
    }
    
    public void setPassword(final String userId, final String password, final String oldPassword, final AuthToken token) throws Exception {
        final MetaInfo.User oldUser = this.getUser(userId);
        if (oldUser == null) {
            throw new SecurityException("User " + userId + " old password was incorrect");
        }
        final byte[] salt = oldUser.uuid.toEightBytes();
        if (oldPassword != null) {
            final String oldEncrypted = encrypt(oldPassword, salt);
            if (!oldEncrypted.equals(oldUser.getEncryptedPassword())) {
                throw new SecurityException("User " + userId + " old password was incorrect");
            }
        }
        final String encrypted = encrypt(password, salt);
        oldUser.setEncryptedPassword(encrypted);
        this.metadataRepository.putMetaObject(oldUser, token);
    }
    
    public AuthToken authenticate(final String userId, final String password) throws Exception {
        return this.authenticate(userId, password, null, null);
    }
    
    public AuthToken authenticate(final String userId, final String password, final String clientId, final String type) throws Exception {
        this.refreshInternalMaps(userId);
        final IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
        final MetaInfo.User oldUser = (MetaInfo.User)this.metadataRepository.getMetaObjectByName(EntityType.USER, "Global", userId, null, HSecurityManager.TOKEN);
        if (oldUser == null) {
            throw new SecurityException("User " + userId + " does not exist or password is incorrect");
        }
        AuthToken token = null;
        if (oldUser.getOriginType().equals(MetaInfo.User.AUTHORIZATION_TYPE.LDAP)) {
            final String name = (oldUser.getAlias() != null) ? oldUser.getAlias() : userId;
            if (this.authenticateLDAP(name, password, oldUser.getLdap())) {
                token = new AuthToken();
                final SessionInfo session = new SessionInfo(userId, System.currentTimeMillis(), clientId, type);
                authTokens.put(token, session);
                if (HSecurityManager.logger.isInfoEnabled()) {
                    HSecurityManager.logger.info(("successfully logged in ldap user : " + userId));
                }
                return token;
            }
            throw new SecurityException("User " + userId + " does not exist or password is incorrect");
        }
        else {
            final byte[] salt = oldUser.uuid.toEightBytes();
            final String encrypted = encrypt(password, salt);
            if (encrypted.equals(oldUser.getEncryptedPassword())) {
                token = new AuthToken();
                final SessionInfo session2 = new SessionInfo(userId, System.currentTimeMillis(), clientId, type);
                authTokens.put(token, session2);
                if (HSecurityManager.logger.isInfoEnabled()) {
                    HSecurityManager.logger.info(("successfully logged in user : " + userId));
                }
                return token;
            }
            throw new SecurityException("User " + userId + " does not exist or password is incorrect");
        }
    }
    
    private boolean authenticateLDAP(final String userId, final String password, final String ldap) throws Exception {
        final AuthLayer al = LDAPAuthLayer.get(ldap);
        final Password p = new Password();
        p.setPlain(password);
        return al.getAccess(userId, p);
    }
    
    public boolean isAuthenticated(final AuthToken token) throws SecurityException {
        final IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
        return token != null && authTokens.containsKey(token);
    }
    
    public static String getAutheticatedUserName(final AuthToken token) throws SecurityException {
        final IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
        if (authTokens.get(token) != null) {
            return ((SessionInfo)authTokens.get(token)).userId;
        }
        return null;
    }
    
    public MetaInfo.User getAuthenticatedUser(final AuthToken token) throws SecurityException, MetaDataRepositoryException, InvalidTokenException {
        if (!this.isAuthenticated(token)) {
            throw new InvalidTokenException("InvalidTokenException: Invalid token");
        }
        final String uid = getAutheticatedUserName(token);
        return this.getUser(uid);
    }
    
    public boolean userAllowed(final MetaInfo.User user, final ObjectPermission permission) throws MetaDataRepositoryException {
        final List<ObjectPermission> perms = user.getAllPermissions();
        if (perms != null) {
            if (HSecurityManager.logger.isTraceEnabled()) {
                HSecurityManager.logger.trace(("No of permissions user have : " + perms.size()));
            }
            for (final ObjectPermission perm : perms) {
                if (HSecurityManager.logger.isTraceEnabled()) {
                    HSecurityManager.logger.trace(("checking if user's existing permission : '" + perm + "', implies the required permission : '" + permission + "'"));
                }
                if (perm.implies(permission)) {
                    if (HSecurityManager.logger.isDebugEnabled()) {
                        HSecurityManager.logger.debug(("YES - user is allowed permission : " + perm));
                    }
                    return true;
                }
            }
        }
        return false;
    }
    
    private void checkCreatePermission(final MetaInfo.Role user, final ObjectPermission permission) throws MetaDataRepositoryException {
        if (user == null) {
            return;
        }
        if (permission == null) {
            return;
        }
        if (permission.getActions() == null) {
            return;
        }
        if (permission.getActions().contains(ObjectPermission.Action.create) && permission.getActions().contains(ObjectPermission.Action.read)) {
            return;
        }
        final List<ObjectPermission> perms = user.getAllPermissions();
        if (permission.getActions().contains(ObjectPermission.Action.create)) {
            for (final ObjectPermission rolePerms : perms) {
                boolean actionMatch = false;
                boolean domainMatch = true;
                boolean objectTypeMatch = true;
                boolean objectNameMatch = true;
                if (rolePerms.getActions() != null && !rolePerms.getActions().contains(ObjectPermission.Action.read)) {
                    continue;
                }
                if (rolePerms.getActions() == null) {
                    actionMatch = true;
                }
                if (rolePerms.getActions() != null && rolePerms.getActions().contains(ObjectPermission.Action.read)) {
                    actionMatch = true;
                }
                if (rolePerms.getDomains() == null) {
                    domainMatch = true;
                }
                if (rolePerms.getDomains() != null) {
                    for (final String domain : permission.getDomains()) {
                        if (!rolePerms.getDomains().contains(domain)) {
                            domainMatch = false;
                        }
                    }
                }
                if (rolePerms.getObjectTypes() == null) {
                    objectTypeMatch = true;
                }
                else {
                    for (final ObjectPermission.ObjectType ot : permission.getObjectTypes()) {
                        if (!rolePerms.getObjectTypes().contains(ot)) {
                            objectTypeMatch = false;
                        }
                    }
                }
                if (rolePerms.getObjectNames() == null) {
                    objectNameMatch = true;
                }
                else {
                    for (final String on : permission.getObjectNames()) {
                        if (!rolePerms.getObjectNames().contains(on)) {
                            objectNameMatch = false;
                        }
                    }
                }
                if (actionMatch && domainMatch && objectTypeMatch && objectNameMatch) {
                    return;
                }
            }
            throw new SecurityException("Need to grant read permission before create permission.");
        }
    }
    
    private boolean userDisallowed(final MetaInfo.User user, final ObjectPermission permission) throws MetaDataRepositoryException {
        final List<ObjectPermission> perms = user.getAllPermissions();
        if (perms != null) {
            if (HSecurityManager.logger.isTraceEnabled()) {
                HSecurityManager.logger.trace(("No of permissions user have : " + perms.size()));
            }
            final ObjectPermission disallowPermission = new ObjectPermission(permission, ObjectPermission.PermissionType.disallow);
            for (final ObjectPermission perm : perms) {
                if (perm.implies(disallowPermission)) {
                    if (HSecurityManager.logger.isDebugEnabled()) {
                        HSecurityManager.logger.debug(("isPermitted - user is exclusive dis-allowed " + perm));
                    }
                    return true;
                }
            }
        }
        return false;
    }
    
    private boolean specialCase(final AuthToken token, final ObjectPermission permission) {
        final Set<ObjectPermission.ObjectType> types = permission.getObjectTypes();
        if (types != null) {
            for (final ObjectPermission.ObjectType type : types) {
                if (type.equals(ObjectPermission.ObjectType.server)) {
                    return true;
                }
            }
        }
        return token.equals(HSecurityManager.TOKEN) || HSecurityManager.FIRSTTIME;
    }
    
    public boolean isDisallowed(final AuthToken token, final ObjectPermission permission) throws SecurityException, MetaDataRepositoryException {
        if (HSecurityManager.logger.isTraceEnabled()) {
            HSecurityManager.logger.trace(("isDisallowed - checking if user specially does not have Permission : " + permission));
        }
        if (this.specialCase(token, permission)) {
            return false;
        }
        final MetaInfo.User actualUser = this.getAuthenticatedUser(token);
        if (HSecurityManager.logger.isTraceEnabled()) {
            HSecurityManager.logger.trace(("isDisallowed - user : " + actualUser.name));
        }
        return this.userDisallowed(actualUser, permission);
    }
    
    public boolean isAllowed(final AuthToken token, final ObjectPermission permission) throws SecurityException, MetaDataRepositoryException {
        if (HSecurityManager.logger.isTraceEnabled()) {
            HSecurityManager.logger.trace(("isAllowed - checking if user have Permission : " + permission));
        }
        if (this.specialCase(token, permission)) {
            return true;
        }
        final MetaInfo.User actualUser = this.getAuthenticatedUser(token);
        if (HSecurityManager.logger.isTraceEnabled()) {
            HSecurityManager.logger.trace(("isAllowed - user : " + actualUser.name));
        }
        return this.userAllowed(actualUser, permission);
    }
    
    public boolean isAllowedAndNotDisallowed(final AuthToken token, final ObjectPermission permission) throws SecurityException, MetaDataRepositoryException {
        if (HSecurityManager.logger.isTraceEnabled()) {
            HSecurityManager.logger.trace(("isPermittedAndNotDisallowed - checking if user have Permission : " + permission));
        }
        if (this.specialCase(token, permission)) {
            return true;
        }
        final MetaInfo.User actualUser = this.getAuthenticatedUser(token);
        if (HSecurityManager.logger.isTraceEnabled()) {
            HSecurityManager.logger.trace(("isPermittedAndNotDisallowed - user : " + actualUser.name));
        }
        return !this.userDisallowed(actualUser, permission) && this.userAllowed(actualUser, permission);
    }
    
    public void logout(final AuthToken token) {
        final IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
        if (authTokens.containsKey(token)) {
            authTokens.remove(token);
        }
    }
    
    public boolean userExists(final String userId) throws SecurityException, MetaDataRepositoryException {
        return this.getUser(userId) != null;
    }
    
    public void refreshInternalMaps(final String userId) {
        if (userId == null) {
            if (HSecurityManager.logger.isTraceEnabled()) {
                HSecurityManager.logger.trace("clearing userMap.");
            }
            HSecurityManager.userMap.clear();
        }
        else {
            if (HSecurityManager.logger.isTraceEnabled()) {
                HSecurityManager.logger.trace(("clearing " + userId + " from userMap."));
            }
            HSecurityManager.userMap.remove(userId);
        }
    }
    
    public MetaInfo.User getUser(final String userId) throws SecurityException, MetaDataRepositoryException {
        if (HSecurityManager.userMap.get(userId) != null) {
            return HSecurityManager.userMap.get(userId);
        }
        final MetaInfo.User u = (MetaInfo.User)this.metadataRepository.getMetaObjectByName(EntityType.USER, "Global", userId, null, HSecurityManager.TOKEN);
        if (u == null) {
            return u;
        }
        u.refreshPermissions();
        HSecurityManager.userMap.put(userId, u);
        return u;
    }
    
    public void addUser(final MetaInfo.User user, final AuthToken token) throws MetaDataRepositoryException {
        if (user != null && user.getLdap() != null) {
            this.addUser(user, token, user.getLdap());
            return;
        }
        final String uid = user.getUserId();
        if (HSecurityManager.logger.isDebugEnabled()) {
            HSecurityManager.logger.debug(("adding a user : " + uid + ", " + token));
        }
        final MetaInfo.User u = (MetaInfo.User)this.metadataRepository.getMetaObjectByName(EntityType.USER, "Global", uid, null, HSecurityManager.TOKEN);
        if (u == null) {
            this.metadataRepository.putMetaObject(user, token);
            return;
        }
        throw new SecurityException("User " + uid + " already exists.");
    }
    
    public void addUser(final MetaInfo.User user, final AuthToken token, final String ldap) throws MetaDataRepositoryException {
        String uid = user.getUserId();
        if (HSecurityManager.logger.isDebugEnabled()) {
            HSecurityManager.logger.debug(("adding a user : " + uid + ", " + token));
        }
        final MetaInfo.User u = (MetaInfo.User)this.metadataRepository.getMetaObjectByName(EntityType.USER, "Global", uid, null, HSecurityManager.TOKEN);
        if (u == null) {
            if (ldap != null) {
                if (user.getAlias() != null) {
                    uid = user.getAlias();
                }
                if (!this.ldapUserExists(uid, ldap)) {
                    throw new SecurityException(("User " + user.getAlias() != null) ? user.getAlias() : (uid + " does not exist in ldap system :" + ldap));
                }
                final MetaInfo.PropertySet ldapPropSet = (MetaInfo.PropertySet)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYSET, Utility.splitDomain(ldap), Utility.splitName(ldap), null, HSecurityManager.TOKEN);
                this.metadataRepository.putMetaObject(user, token);
                ldapPropSet.addReverseIndexObjectDependencies(user.getUuid());
                this.metadataRepository.updateMetaObject(ldapPropSet, HSecurityManager.TOKEN);
            }
            return;
        }
        throw new SecurityException("User " + uid + " already exists.");
    }
    
    private boolean ldapUserExists(final String uid, final String ldap) throws MetaDataRepositoryException {
        final AuthLayer al = LDAPAuthLayer.get(ldap);
        return al.find(uid);
    }
    
    public void updateUser(final MetaInfo.User user, final AuthToken token) throws SecurityException, MetaDataRepositoryException {
        final MetaInfo.User oldUser = this.getUser(user.getUserId());
        if (oldUser == null) {
            throw new SecurityException("wrong user id credentials. ");
        }
        final ObjectPermission permission = new ObjectPermission("Global", ObjectPermission.Action.update, ObjectPermission.ObjectType.user, oldUser.name);
        if (this.isAllowedAndNotDisallowed(token, permission)) {
            if (HSecurityManager.logger.isDebugEnabled()) {
                HSecurityManager.logger.debug(("updating user :" + oldUser.getUserId()));
            }
            oldUser.setFirstName(user.getFirstName());
            oldUser.setLastName(user.getLastName());
            oldUser.setMainEmail(user.getMainEmail());
            oldUser.setEncryptedPassword(user.getEncryptedPassword());
            oldUser.setContactMechanisms(user.getContactMechanisms());
            oldUser.setUserTimeZone(user.getUserTimeZone());
            if (HSecurityManager.logger.isDebugEnabled()) {
                HSecurityManager.logger.debug("BEFORE ROLES { \n");
                HSecurityManager.logger.debug(("NEW user :" + user.getRoles()));
                HSecurityManager.logger.debug(("OLD user :" + oldUser.getRoles()));
                HSecurityManager.logger.debug(" } \n");
            }
            oldUser.setRoles(user.getRoles());
            if (HSecurityManager.logger.isDebugEnabled()) {
                HSecurityManager.logger.debug("AFTER ROLES { \n");
                HSecurityManager.logger.debug(("NEW user :" + user.getRoles()));
                HSecurityManager.logger.debug(("OLD user :" + oldUser.getRoles()));
                HSecurityManager.logger.debug(" } \n");
            }
            if (HSecurityManager.logger.isDebugEnabled()) {
                HSecurityManager.logger.debug("BEFORE PERMISSIONS { \n");
                HSecurityManager.logger.debug(("NEW user :" + user.getPermissions()));
                HSecurityManager.logger.debug(("OLD user :" + oldUser.getPermissions()));
                HSecurityManager.logger.debug(" } \n");
            }
            oldUser.setPermissions(user.getPermissions());
            if (HSecurityManager.logger.isDebugEnabled()) {
                HSecurityManager.logger.debug("AFTER PERMISSIONS { \n");
                HSecurityManager.logger.debug(("NEW user :" + user.getPermissions()));
                HSecurityManager.logger.debug(("OLD user :" + oldUser.getPermissions()));
                HSecurityManager.logger.debug(" } \n");
            }
            this.metadataRepository.updateMetaObject(oldUser, token);
            return;
        }
        throw new SecurityException("No permissions to update the user : " + user);
    }
    
    public void removeUser(final String callerId, final String toBeDeletedId, final AuthToken token) throws MetaDataRepositoryException {
        if (callerId == null || toBeDeletedId == null) {
            throw new SecurityException("Cannot remove user!");
        }
        MetaInfo.User oldUser;
        try {
            oldUser = this.getUser(toBeDeletedId);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
        if (oldUser == null) {
            throw new SecurityException("can not remove, user " + toBeDeletedId + " credentials. ");
        }
        if (callerId.equals(toBeDeletedId)) {
            throw new SecurityException("Users unable to remove themselves!");
        }
        if (oldUser.getUserId().equalsIgnoreCase("admin")) {
            throw new SecurityException("can not remove, user " + toBeDeletedId + ". ");
        }
        this.metadataRepository.removeMetaObjectByName(EntityType.USER, "Global", oldUser.getUserId(), null, token);
        this.refreshInternalMaps(toBeDeletedId);
    }
    
    public void addContactMechanism(final String userId, final MetaInfo.ContactMechanism.ContactType type, final String data, final AuthToken token) throws SecurityException, MetaDataRepositoryException {
        final MetaInfo.User oldUser = this.getUser(userId);
        oldUser.addContactMechanism(type, data);
        try {
            this.metadataRepository.updateMetaObject(oldUser, token);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void updateContactMechanism(final String userId, final int index, final MetaInfo.ContactMechanism.ContactType type, final String data, final AuthToken token) throws MetaDataRepositoryException {
        final MetaInfo.User oldUser = this.getUser(userId);
        oldUser.updateContactMechanism(index, type, data);
        try {
            this.metadataRepository.updateMetaObject(oldUser, token);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void removeContactMechanism(final String userId, final int index, final AuthToken token) throws MetaDataRepositoryException {
        final MetaInfo.User oldUser = this.getUser(userId);
        oldUser.removeContactMechanism(index);
        try {
            this.metadataRepository.updateMetaObject(oldUser, token);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void setDefaultContactMechanism(final String userId, final int index, final AuthToken token) throws MetaDataRepositoryException {
        final MetaInfo.User oldUser = this.getUser(userId);
        oldUser.setDefaultContactMechanism(index);
        try {
            this.metadataRepository.updateMetaObject(oldUser, token);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public Set<MetaInfo.User> getUsers(final AuthToken token) {
        try {
            return (Set<MetaInfo.User>)this.metadataRepository.getByEntityType(EntityType.USER, token);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public List<String> getPermissionDomains(final AuthToken token) throws SecurityException {
        final List<String> ret = new ArrayList<String>();
        ret.add("system");
        List<MetaInfo.Namespace> apps = null;
        try {
            apps = (List<MetaInfo.Namespace>)(List)MetadataRepository.getINSTANCE().getByEntityType(EntityType.NAMESPACE, token);
        }
        catch (Exception e) {
            throw new SecurityException(e.getMessage());
        }
        for (final MetaInfo.Namespace app : apps) {
            ret.add(app.nsName);
        }
        return ret;
    }
    
    public List<ObjectPermission.Action> getPermissionActions() {
        return Arrays.asList(ObjectPermission.Action.values());
    }
    
    public List<ObjectPermission.ObjectType> getPermissionObjectTypes() {
        return Arrays.asList(ObjectPermission.ObjectType.values());
    }
    
    public void addRole(final MetaInfo.Role role, final AuthToken token) {
        try {
            final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, role.getNsName(), role.getName(), null, token);
            if (actualRole != null) {
                throw new SecurityException("Role " + role + " exist");
            }
            this.metadataRepository.putMetaObject(role, token);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public String[] roleAndUserParser(final String roleOrUser) {
        final String[] domainAndName = new String[2];
        if (roleOrUser.indexOf(".") > -1) {
            final String[] splitName = roleOrUser.split("\\.");
            domainAndName[0] = splitName[0];
            domainAndName[1] = splitName[1];
        }
        else if (roleOrUser.indexOf(":") > -1) {
            final String[] splitName = roleOrUser.split("\\:");
            domainAndName[0] = splitName[0];
            domainAndName[1] = splitName[1];
        }
        else {
            domainAndName[0] = "Global";
            domainAndName[1] = roleOrUser;
        }
        return domainAndName;
    }
    
    public MetaInfo.Role getRole(final String role) {
        final String[] domainAndRole = this.roleAndUserParser(role);
        try {
            return (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public MetaInfo.Role getRole(final String domain, final String roleName) {
        try {
            return (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domain, roleName, null, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void removeRole(final MetaInfo.Role role, final AuthToken token) {
        if (role == null) {
            throw new SecurityException("Cannot drop role!");
        }
        try {
            final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, role.getNsName(), role.getName(), null, HSecurityManager.TOKEN);
            if (actualRole == null) {
                throw new SecurityException("Role " + role + " does NOT exist");
            }
            this.metadataRepository.removeMetaObjectByUUID(actualRole.getUuid(), token);
            this.refreshInternalMaps(null);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void removeRoles(final List<MetaInfo.Role> roles, final AuthToken token) throws MetaDataRepositoryException {
        if (roles == null || roles.size() == 0) {
            HSecurityManager.logger.warn("empty roles list supplied");
            return;
        }
        final List<MetaInfo.Role> rolesCantBeRemoved = new ArrayList<MetaInfo.Role>();
        for (final MetaInfo.Role role : roles) {
            final ObjectPermission p = new ObjectPermission(role.nsName, ObjectPermission.Action.drop, ObjectPermission.ObjectType.role, role.name);
            if (this.isAllowedAndNotDisallowed(token, p)) {
                this.removeRole(role, token);
            }
            else {
                rolesCantBeRemoved.add(role);
            }
        }
        if (rolesCantBeRemoved.size() > 0) {
            HSecurityManager.logger.warn(("User has no permission to drop these roles : " + Arrays.deepToString(rolesCantBeRemoved.toArray())));
        }
    }
    
    public void grantRolePermission(final String role, final ObjectPermission permission, final AuthToken token) {
        if (role == null || permission == null) {
            throw new SecurityException("null values passed.");
        }
        final String[] domainAndRole = this.roleAndUserParser(role);
        try {
            final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
            if (actualRole == null) {
                throw new SecurityException("Role " + role + "does not existing. ");
            }
            final Set<ObjectPermission.Action> a = new HashSet<ObjectPermission.Action>();
            a.add(ObjectPermission.Action.grant);
            final ObjectPermission grantPerm = new ObjectPermission(permission.getDomains(), a, permission.getObjectTypes(), permission.getObjectNames());
            if (!this.isAllowedAndNotDisallowed(token, grantPerm)) {
                throw new SecurityException("Not enough permissions to grant : " + permission.toString());
            }
            this.checkCreatePermission(actualRole, permission);
            actualRole.grantPermission(permission);
            this.metadataRepository.updateMetaObject(actualRole, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void grantRolePermissions(final String role, final List<String> permissionsList, final AuthToken token) {
        if (role == null || permissionsList == null || permissionsList.size() == 0) {
            throw new SecurityException("null values passed.");
        }
        final String[] domainAndRole = this.roleAndUserParser(role);
        try {
            final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
            if (actualRole == null) {
                throw new SecurityException("Role " + role + "does not existing. ");
            }
            final List<ObjectPermission> allowedPermsList = new ArrayList<ObjectPermission>();
            final List<ObjectPermission> disAllowedPermsList = new ArrayList<ObjectPermission>();
            final List<String> disAllowedPermsStr = new ArrayList<String>();
            for (final String perm : permissionsList) {
                try {
                    final ObjectPermission p = new ObjectPermission(perm);
                    allowedPermsList.add(p);
                }
                catch (IllegalArgumentException ie) {
                    disAllowedPermsStr.add(perm);
                }
            }
            if (disAllowedPermsStr.size() > 0) {
                throw new SecurityException("Wrong permission strings are provided : " + Arrays.deepToString(disAllowedPermsStr.toArray()));
            }
            for (final ObjectPermission ap : allowedPermsList) {
                final Set<ObjectPermission.Action> a = new HashSet<ObjectPermission.Action>();
                a.add(ObjectPermission.Action.grant);
                final ObjectPermission grantPerm = new ObjectPermission(ap.getDomains(), a, ap.getObjectTypes(), ap.getObjectNames());
                if (!this.isAllowedAndNotDisallowed(token, grantPerm)) {
                    disAllowedPermsList.add(ap);
                }
            }
            if (disAllowedPermsList.size() > 0) {
                throw new SecurityException("Not enough permissions to grant : " + Arrays.deepToString(disAllowedPermsList.toArray()));
            }
            actualRole.grantPermissions(allowedPermsList);
            this.metadataRepository.updateMetaObject(actualRole, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void revokeRolePermission(final String role, final ObjectPermission permission, final AuthToken token) {
        if (role == null || permission == null) {
            throw new SecurityException("null values passed.");
        }
        final String[] domainAndRole = this.roleAndUserParser(role);
        try {
            MetaInfo.Role actualRole = null;
            actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
            if (actualRole == null) {
                throw new SecurityException("Null role is passed : " + role);
            }
            final Set<ObjectPermission.Action> a = new HashSet<ObjectPermission.Action>();
            a.add(ObjectPermission.Action.grant);
            final ObjectPermission revPerm = new ObjectPermission(permission.getDomains(), a, permission.getObjectTypes(), permission.getObjectNames());
            if (!this.isAllowedAndNotDisallowed(token, revPerm)) {
                throw new SecurityException("Not enough permissions to revoke : " + permission);
            }
            actualRole.revokePermission(permission);
            this.metadataRepository.updateMetaObject(actualRole, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void revokeRolePermissions(final String role, final List<String> permissionsList, final AuthToken token) {
        if (role == null || permissionsList == null) {
            throw new SecurityException("null values passed.");
        }
        final String[] domainAndRole = this.roleAndUserParser(role);
        try {
            final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
            if (actualRole == null) {
                throw new SecurityException("Role " + role + "does not existing. ");
            }
            final List<ObjectPermission> allowedPermsList = new ArrayList<ObjectPermission>();
            final List<ObjectPermission> disAllowedPermsList = new ArrayList<ObjectPermission>();
            final List<String> disAllowedPermsStr = new ArrayList<String>();
            for (final String perm : permissionsList) {
                try {
                    final ObjectPermission p = new ObjectPermission(perm);
                    allowedPermsList.add(p);
                }
                catch (IllegalArgumentException ie) {
                    disAllowedPermsStr.add(perm);
                }
            }
            if (disAllowedPermsStr.size() > 0) {
                throw new SecurityException("Wrong permission strings are provided : " + Arrays.deepToString(disAllowedPermsStr.toArray()));
            }
            for (final ObjectPermission ap : allowedPermsList) {
                final Set<ObjectPermission.Action> a = new HashSet<ObjectPermission.Action>();
                a.add(ObjectPermission.Action.grant);
                final ObjectPermission revokPerm = new ObjectPermission(ap.getDomains(), a, ap.getObjectTypes(), ap.getObjectNames());
                if (!this.isAllowedAndNotDisallowed(token, revokPerm)) {
                    disAllowedPermsList.add(ap);
                }
            }
            if (disAllowedPermsList.size() > 0) {
                throw new SecurityException("Not enough permissions to revoke : " + Arrays.deepToString(disAllowedPermsList.toArray()));
            }
            actualRole.revokePermissions(allowedPermsList);
            this.metadataRepository.updateMetaObject(actualRole, HSecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e) {
            throw new SecurityException(e.getMessage());
        }
    }
    
    public void grantRoleRole(final String role, final String subRole, final AuthToken token) throws MetaDataRepositoryException {
        if (role == null || subRole == null) {
            throw new SecurityException("null values are passed. ");
        }
        final String[] domainAndRole = this.roleAndUserParser(role);
        final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
        final String[] domainAndSubRole = this.roleAndUserParser(role);
        final MetaInfo.Role actualSubRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndSubRole[0], domainAndSubRole[1], null, HSecurityManager.TOKEN);
        if (actualRole == null || actualSubRole == null) {
            throw new SecurityException("null roles are supplied.");
        }
        if (actualRole.uuid.equals(actualSubRole.uuid)) {
            throw new SecurityException("can not add role to itself ");
        }
        final ObjectPermission roleGrantPerm = new ObjectPermission(actualSubRole.getDomain(), ObjectPermission.Action.grant, ObjectPermission.ObjectType.role, actualSubRole.getRoleName());
        if (!this.isAllowedAndNotDisallowed(token, roleGrantPerm)) {
            throw new SecurityException("Not enough permissions to grant role : " + subRole);
        }
        actualRole.grantRole(actualSubRole);
        this.metadataRepository.updateMetaObject(actualRole, HSecurityManager.TOKEN);
    }
    
    public void grantRoleRoles(final String role, final List<String> subRoles, final AuthToken token) throws Exception {
        if (role == null || subRoles == null || subRoles.size() == 0) {
            throw new SecurityException("null or empty roles passed");
        }
        final String[] domainAndRole = this.roleAndUserParser(role);
        MetaInfo.Role actualRole = null;
        actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
        if (actualRole == null) {
            throw new SecurityException("non existing role is passed");
        }
        final List<MetaInfo.Role> actualSubRoles = new ArrayList<MetaInfo.Role>();
        final List<String> nonExsSubRoles = new ArrayList<String>();
        for (final String sr : subRoles) {
            final String[] domainAndSubRole = this.roleAndUserParser(sr);
            final MetaInfo.Role r = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndSubRole[0], domainAndSubRole[1], null, HSecurityManager.TOKEN);
            if (r == null) {
                nonExsSubRoles.add(sr);
            }
            else {
                actualSubRoles.add(r);
            }
        }
        if (nonExsSubRoles.size() > 0) {
            throw new SecurityException("null/non existing roles are can not be added : " + Arrays.deepToString(nonExsSubRoles.toArray()));
        }
        final List<MetaInfo.Role> notAllowedRoles = new ArrayList<MetaInfo.Role>();
        for (final MetaInfo.Role ars : actualSubRoles) {
            final ObjectPermission p = new ObjectPermission(ars.nsName, ObjectPermission.Action.grant, ObjectPermission.ObjectType.role, ars.name);
            if (!this.isAllowedAndNotDisallowed(token, p)) {
                notAllowedRoles.add(ars);
            }
        }
        if (notAllowedRoles.size() > 0) {
            throw new SecurityException("user cannot grant following roles : " + Arrays.deepToString(notAllowedRoles.toArray()));
        }
        actualRole.grantRoles(actualSubRoles);
        this.metadataRepository.updateMetaObject(actualRole, HSecurityManager.TOKEN);
    }
    
    public void revokeRoleRole(final String role, final String subRole, final AuthToken token) throws Exception {
        if (role == null || subRole == null) {
            throw new SecurityException("null values are passed. ");
        }
        final ArrayList<String> subroles = new ArrayList<String>();
        subroles.add(subRole);
        this.revokeRoleRoles(role, subroles, token);
    }
    
    public void revokeRoleRoles(final String role, final List<String> subRoles, final AuthToken token) throws Exception {
        if (role == null || subRoles == null || subRoles.size() == 0) {
            throw new SecurityException("null role is passed");
        }
        final String[] domainAndRole = this.roleAndUserParser(role);
        final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
        final List<MetaInfo.Role> allowedSubRoles = new ArrayList<MetaInfo.Role>();
        final List<String> unAllowedSubRoles = new ArrayList<String>();
        for (final String sr : subRoles) {
            final String[] domainAndSubRole = this.roleAndUserParser(sr);
            MetaInfo.Role r = null;
            r = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndSubRole[0], domainAndSubRole[1], null, HSecurityManager.TOKEN);
            if (r == null) {
                unAllowedSubRoles.add(sr);
            }
            else {
                allowedSubRoles.add(r);
            }
        }
        if (unAllowedSubRoles.size() > 0) {
            throw new SecurityException("Following roles are NOT existing : " + Arrays.deepToString(unAllowedSubRoles.toArray()));
        }
        for (final MetaInfo.Role asr : allowedSubRoles) {
            final ObjectPermission p = new ObjectPermission(asr.nsName, ObjectPermission.Action.grant, ObjectPermission.ObjectType.role, asr.name);
            if (!this.isAllowedAndNotDisallowed(token, p)) {
                unAllowedSubRoles.add(asr.getRole());
            }
        }
        if (unAllowedSubRoles.size() > 0) {
            throw new SecurityException("User does NOT have enough permissions to revoke following roles : " + Arrays.deepToString(unAllowedSubRoles.toArray()));
        }
        actualRole.revokeRoles(allowedSubRoles);
        this.metadataRepository.updateMetaObject(actualRole, HSecurityManager.TOKEN);
    }
    
    public List<MetaInfo.Role> getRoles(final AuthToken token) throws Exception {
        final List<MetaInfo.Role> allRoles = new ArrayList<MetaInfo.Role>((Collection<? extends MetaInfo.Role>)this.metadataRepository.getByEntityType(EntityType.ROLE, token));
        return allRoles;
    }
    
    public void grantUserPermission(final String user, final ObjectPermission permission, final AuthToken token) throws Exception {
        if (user == null || permission == null) {
            throw new SecurityException("null values passed. ");
        }
        final MetaInfo.User actualUser = this.getUser(user);
        if (actualUser == null) {
            throw new SecurityException("non existing user passed. ");
        }
        final Set<ObjectPermission.Action> a = new HashSet<ObjectPermission.Action>();
        a.add(ObjectPermission.Action.grant);
        final ObjectPermission grantPerm = new ObjectPermission(permission.getDomains(), a, permission.getObjectTypes(), permission.getObjectNames());
        if (!this.isAllowedAndNotDisallowed(token, grantPerm)) {
            throw new SecurityException("User does NOT enough permissions to grant : " + permission.toString());
        }
        actualUser.grantPermission(permission);
        this.metadataRepository.updateMetaObject(actualUser, HSecurityManager.TOKEN);
    }
    
    public void grantUserPermissions(final String user, final List<String> permissionsList, final AuthToken token) throws Exception {
        if (user == null || permissionsList == null || permissionsList.size() == 0) {
            throw new SecurityException("null values passed.");
        }
        final MetaInfo.User actualUser = this.getUser(user);
        if (actualUser == null) {
            throw new SecurityException("user " + user + " does not exists.");
        }
        final List<ObjectPermission> allowedPermsList = new ArrayList<ObjectPermission>();
        final List<ObjectPermission> disAllowedPermsList = new ArrayList<ObjectPermission>();
        final List<String> disAllowedPermsStr = new ArrayList<String>();
        for (final String perm : permissionsList) {
            try {
                final ObjectPermission p = new ObjectPermission(perm);
                allowedPermsList.add(p);
            }
            catch (IllegalArgumentException ie) {
                disAllowedPermsStr.add(perm);
            }
        }
        if (disAllowedPermsStr.size() > 0) {
            throw new SecurityException("Wrong permission strings are provided : " + Arrays.deepToString(disAllowedPermsStr.toArray()));
        }
        for (final ObjectPermission ap : allowedPermsList) {
            final Set<ObjectPermission.Action> a = new HashSet<ObjectPermission.Action>();
            a.add(ObjectPermission.Action.grant);
            final ObjectPermission grantPerm = new ObjectPermission(ap.getDomains(), a, ap.getObjectTypes(), ap.getObjectNames());
            if (!this.isAllowedAndNotDisallowed(token, grantPerm)) {
                disAllowedPermsList.add(ap);
            }
        }
        if (disAllowedPermsList.size() > 0) {
            throw new SecurityException("Not enough permissions to grant : " + Arrays.deepToString(disAllowedPermsList.toArray()));
        }
        actualUser.grantPermissions(allowedPermsList);
        this.metadataRepository.updateMetaObject(actualUser, HSecurityManager.TOKEN);
    }
    
    public void revokeUserPermission(final String user, final ObjectPermission permission, final AuthToken token) throws Exception {
        if (user == null || permission == null) {
            throw new SecurityException("null values passed. ");
        }
        final MetaInfo.User actualUser = this.getUser(user);
        if (actualUser == null) {
            throw new SecurityException("non existing user passed. ");
        }
        final Set<ObjectPermission.Action> a = new HashSet<ObjectPermission.Action>();
        a.add(ObjectPermission.Action.grant);
        final ObjectPermission revokePerm = new ObjectPermission(permission.getDomains(), a, permission.getObjectTypes(), permission.getObjectNames());
        if (!this.isAllowedAndNotDisallowed(token, revokePerm)) {
            throw new SecurityException("User does NOT enough permissions to revoke : " + permission.toString());
        }
        actualUser.revokePermission(permission);
        this.metadataRepository.updateMetaObject(actualUser, HSecurityManager.TOKEN);
    }
    
    public void revokeUserPermissions(final String user, final List<String> permissionsList, final AuthToken token) throws Exception {
        if (user == null || permissionsList == null || permissionsList.size() == 0) {
            throw new SecurityException("null values passed.");
        }
        final MetaInfo.User actualUser = this.getUser(user);
        if (actualUser == null) {
            throw new SecurityException("user " + user + " does not exists.");
        }
        final List<ObjectPermission> allowedPermsList = new ArrayList<ObjectPermission>();
        final List<ObjectPermission> disAllowedPermsList = new ArrayList<ObjectPermission>();
        final List<String> disAllowedPermsStr = new ArrayList<String>();
        for (final String perm : permissionsList) {
            try {
                final ObjectPermission p = new ObjectPermission(perm);
                allowedPermsList.add(p);
            }
            catch (IllegalArgumentException ie) {
                disAllowedPermsStr.add(perm);
            }
        }
        if (disAllowedPermsStr.size() > 0) {
            throw new SecurityException("Wrong permission strings are provided : " + Arrays.deepToString(disAllowedPermsStr.toArray()));
        }
        for (final ObjectPermission ap : allowedPermsList) {
            final Set<ObjectPermission.Action> a = new HashSet<ObjectPermission.Action>();
            a.add(ObjectPermission.Action.grant);
            final ObjectPermission grantPerm = new ObjectPermission(ap.getDomains(), a, ap.getObjectTypes(), ap.getObjectNames());
            if (!this.isAllowedAndNotDisallowed(token, grantPerm)) {
                disAllowedPermsList.add(ap);
            }
        }
        if (disAllowedPermsList.size() > 0) {
            throw new SecurityException("Not enough permissions to grant : " + Arrays.deepToString(disAllowedPermsList.toArray()));
        }
        actualUser.revokePermissions(allowedPermsList);
        this.metadataRepository.updateMetaObject(actualUser, HSecurityManager.TOKEN);
    }
    
    public void grantUserRole(final String userID, final String roleID, final AuthToken token) throws Exception {
        if (userID == null || roleID == null) {
            throw new SecurityException("user or role are null");
        }
        final MetaInfo.User actualUser = this.getUser(userID);
        final String[] domainAndRole = this.roleAndUserParser(roleID);
        final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
        if (actualUser == null || actualRole == null) {
            throw new SecurityException("user or role are null");
        }
        final ObjectPermission roleGrantPerm = new ObjectPermission(actualRole.nsName, ObjectPermission.Action.grant, ObjectPermission.ObjectType.role, actualRole.name);
        if (!this.isAllowedAndNotDisallowed(token, roleGrantPerm)) {
            throw new SecurityException("Not enough permission to grant role : " + actualRole.getRole());
        }
        actualUser.grantRole(actualRole);
        this.metadataRepository.updateMetaObject(actualUser, HSecurityManager.TOKEN);
    }
    
    public void grantUserRoles(final String user, final List<String> roles, final AuthToken token) throws Exception {
        if (user == null || roles == null || roles.size() == 0) {
            throw new SecurityException("user or role are null");
        }
        final MetaInfo.User actualUser = this.getUser(user);
        if (actualUser == null) {
            throw new SecurityException("User " + user + " does not exists.");
        }
        final List<MetaInfo.Role> actualRoles = new ArrayList<MetaInfo.Role>();
        final List<String> nonExistingRoles = new ArrayList<String>();
        for (final String r1 : roles) {
            final String[] domainAndRole = this.roleAndUserParser(r1);
            final MetaInfo.Role r2 = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
            if (r2 == null) {
                nonExistingRoles.add(r1);
            }
            else {
                actualRoles.add(r2);
            }
        }
        if (nonExistingRoles.size() > 0) {
            throw new SecurityException("following roles are not existing : " + Arrays.deepToString(nonExistingRoles.toArray()));
        }
        final List<MetaInfo.Role> notAlowedRoles = new ArrayList<MetaInfo.Role>();
        for (final MetaInfo.Role r3 : actualRoles) {
            final ObjectPermission p = new ObjectPermission(r3.nsName, ObjectPermission.Action.grant, ObjectPermission.ObjectType.role, r3.name);
            if (!this.isAllowedAndNotDisallowed(token, p)) {
                notAlowedRoles.add(r3);
            }
        }
        if (notAlowedRoles.size() > 0) {
            throw new SecurityException("user has no permission to grant these roles : " + Arrays.deepToString(notAlowedRoles.toArray()));
        }
        actualUser.grantRoles(actualRoles);
        this.metadataRepository.updateMetaObject(actualUser, HSecurityManager.TOKEN);
    }
    
    public void revokeUserRole(final String user, final String role, final AuthToken token) throws Exception {
        if (user == null || role == null) {
            throw new SecurityException("user or role are null");
        }
        final ArrayList<String> roleList = new ArrayList<String>();
        roleList.add(role);
        this.revokeUserRoles(user, roleList, token);
    }
    
    public void revokeUserRoles(final String user, final List<String> roles, final AuthToken token) throws Exception {
        if (user == null || (roles == null | roles.size() == 0)) {
            throw new SecurityException("non-existing/null values are provided");
        }
        final MetaInfo.User actualUser = this.getUser(user);
        final List<MetaInfo.Role> actualRoles = new ArrayList<MetaInfo.Role>();
        final List<String> nonExistingRoles = new ArrayList<String>();
        for (final String r1 : roles) {
            final String[] domainAndRole = this.roleAndUserParser(r1);
            MetaInfo.Role r2 = null;
            r2 = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
            if (r2 == null) {
                nonExistingRoles.add(r1);
            }
            else {
                actualRoles.add(r2);
            }
        }
        if (nonExistingRoles.size() > 0) {
            throw new SecurityException("following roles are not existing : " + Arrays.deepToString(nonExistingRoles.toArray()));
        }
        final List<MetaInfo.Role> notAlowedRoles = new ArrayList<MetaInfo.Role>();
        for (final MetaInfo.Role r3 : actualRoles) {
            final ObjectPermission p = new ObjectPermission(r3.nsName, ObjectPermission.Action.grant, ObjectPermission.ObjectType.role, r3.name);
            if (!this.isAllowedAndNotDisallowed(token, p)) {
                notAlowedRoles.add(r3);
            }
        }
        if (notAlowedRoles.size() > 0) {
            throw new SecurityException("user has no permission to revoke these roles : " + Arrays.deepToString(notAlowedRoles.toArray()));
        }
        actualUser.revokeRoles(actualRoles);
        this.metadataRepository.updateMetaObject(actualUser, HSecurityManager.TOKEN);
    }
    
    public void revokeAllUserRoles(final String user, final AuthToken token) throws Exception {
        if (user == null) {
            throw new SecurityException("non-existing/null values are provided");
        }
        final MetaInfo.User actualUser = this.getUser(user);
        final List<MetaInfo.Role> roles = actualUser.getRoles();
        final List<String> roles2 = new ArrayList<String>();
        if (!actualUser.getUserId().equalsIgnoreCase("admin")) {
            for (final MetaInfo.Role role : roles) {
                if (!role.getRole().equalsIgnoreCase("global:admin")) {
                    roles2.add(role.getRole());
                }
            }
        }
        if (roles2.size() > 0) {
            this.revokeUserRoles(actualUser.getUserId(), roles2, token);
        }
    }
    
    public void revokeRoleAllRoles(final String role, final AuthToken token) throws Exception {
        if (role == null) {
            throw new SecurityException("null role is passed");
        }
        final String[] domainAndRole = this.roleAndUserParser(role);
        final MetaInfo.Role actualRole = (MetaInfo.Role)this.metadataRepository.getMetaObjectByName(EntityType.ROLE, domainAndRole[0], domainAndRole[1], null, HSecurityManager.TOKEN);
        if (actualRole == null) {
            throw new SecurityException("non existing role is passed");
        }
        final List<MetaInfo.Role> roles = actualRole.getRoles();
        final List<String> resultantRoles = new ArrayList<String>();
        for (final MetaInfo.Role rol : roles) {
            resultantRoles.add(rol.getRole());
        }
        if (roles != null && roles.size() > 0) {
            this.revokeRoleRoles(role, resultantRoles, token);
        }
    }
    
    static {
        HSecurityManager.logger = Logger.getLogger((Class)HSecurityManager.class);
        PASSWORD = "enfldsgbnlsngdlksdsgm".toCharArray();
        HSecurityManager.FIRSTTIME = true;
        TOKEN = new AuthToken("01e30158-c5ee-85a0-95bd-14109fcf1ed3");
        ENCRYPTION_SALT = new UUID("20a4dd7e-34a6-4f71-bf57-bd8e1285a062");
    }
}
