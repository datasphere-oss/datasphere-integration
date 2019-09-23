package com.datasphere.security;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.IError;
import com.datasphere.errorhandling.DatallRuntimeException;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.utility.Utility;
import com.datasphere.exception.SecurityException;
import com.datasphere.intf.AuthLayer;
import com.datasphere.common.errors.Error;
public class LDAPAuthLayer implements AuthLayer
{
    private static Logger logger;
    private Hashtable<String, Object> env;
    private String INITIAL_CONTEXT_FACTORY;
    private String PROVIDER_URL;
    private String SECURITY_AUTHENTICATION;
    private String SECURITY_PRINCIPAL;
    private String SECURITY_CREDENTIALS;
    private String USER_BASE_DN;
    private String USER_RDN;
    private String USER_USERID;
    private String USER_FIRSTNAME;
    private String USER_LASTNAME;
    private String USER_MAINEMAIL;
    private static Map<String, LDAPAuthLayer> INSTANCES;
    private DirContext ctx;
    
    private LDAPAuthLayer() {
        this.env = null;
        this.INITIAL_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
        this.PROVIDER_URL = null;
        this.SECURITY_AUTHENTICATION = "simple";
        this.SECURITY_PRINCIPAL = null;
        this.SECURITY_CREDENTIALS = null;
        this.USER_BASE_DN = null;
        this.USER_RDN = "cn";
        this.USER_USERID = "uid";
        this.USER_FIRSTNAME = "firstname";
        this.USER_LASTNAME = "lastname";
        this.USER_MAINEMAIL = "mail";
        this.ctx = null;
    }
    
    public static AuthLayer get(String ldap) {
        if (ldap == null || ldap.isEmpty()) {
            return null;
        }
        ldap = ldap.toUpperCase();
        if (LDAPAuthLayer.INSTANCES.get(ldap) == null) {
            synchronized (LDAPAuthLayer.class) {
                if (LDAPAuthLayer.INSTANCES.get(ldap) == null) {
                    MetaInfo.PropertySet ldapPropSet;
                    try {
                        ldapPropSet = (MetaInfo.PropertySet)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYSET, Utility.splitDomain(ldap), Utility.splitName(ldap), null, HSecurityManager.TOKEN);
                    }
                    catch (SecurityException | MetaDataRepositoryException ex3) {
                        LDAPAuthLayer.logger.error((Object)("Failed to fetch LDAP :" + ldap + " details from metadata repository."), (Throwable)ex3);
                        return null;
                    }
                    if (ldapPropSet == null) {
                        LDAPAuthLayer.logger.error((Object)("No LDAP properties found in metadata with name :" + ldap));
                        return null;
                    }
                    checkProperties(ldapPropSet.properties);
                    final LDAPAuthLayer instance = new LDAPAuthLayer();
                    instance.setup(ldapPropSet.properties);
                    LDAPAuthLayer.INSTANCES.put(ldap, instance);
                }
            }
        }
        return LDAPAuthLayer.INSTANCES.get(ldap);
    }
    
    public static void checkProperties(final Map<String, Object> props) {
        final Map<String, Object> tempProps = new Hashtable<String, Object>();
        if (props != null) {
            for (final String key : props.keySet()) {
                tempProps.put(key.toUpperCase(), props.get(key));
            }
        }
        if (tempProps.get("PROVIDER_URL") == null || ((String)tempProps.get("PROVIDER_URL")).isEmpty() || tempProps.get("SECURITY_PRINCIPAL") == null || ((String)tempProps.get("SECURITY_PRINCIPAL")).isEmpty() || tempProps.get("SECURITY_CREDENTIALS") == null || tempProps.get("USER_BASE_DN") == null || ((String)tempProps.get("USER_BASE_DN")).isEmpty() || tempProps.get("USER_RDN") == null || ((String)tempProps.get("USER_RDN")).isEmpty() || tempProps.get("USER_USERID") == null || ((String)tempProps.get("USER_USERID")).isEmpty()) {
            LDAPAuthLayer.logger.error((Object)"LDAP proper set must have PROVIDER_URL, SECURITY_PRINCIPAL, SECURITY_CREDENTIALS, USER_BASE_DN, USER_RDN, USER_USERID.");
            throw new RuntimeException("LDAP proper set must have PROVIDER_URL, SECURITY_PRINCIPAL, SECURITY_CREDENTIALS, USER_BASE_DN, USER_RDN, USER_USERID.");
        }
    }
    
    @Override
    public void setup(final Map<String, Object> props) {
        final Map<String, Object> tempProps = Factory.makeCaseInsensitiveMap();
        if (props != null) {
            tempProps.putAll(props);
        }
        if (tempProps.get("PROVIDER_URL") == null || ((String)tempProps.get("PROVIDER_URL")).isEmpty() || tempProps.get("SECURITY_PRINCIPAL") == null || ((String)tempProps.get("SECURITY_PRINCIPAL")).isEmpty() || tempProps.get("SECURITY_CREDENTIALS") == null || tempProps.get("USER_BASE_DN") == null || ((String)tempProps.get("USER_BASE_DN")).isEmpty() || tempProps.get("USER_RDN") == null || ((String)tempProps.get("USER_RDN")).isEmpty() || tempProps.get("USER_USERID") == null || ((String)tempProps.get("USER_USERID")).isEmpty()) {
            LDAPAuthLayer.logger.error((Object)"LDAP proper set must have PROVIDER_URL, SECURITY_PRINCIPAL, SECURITY_CREDENTIALS, USER_BASE_DN, USER_RDN, USER_USERID.");
            throw new RuntimeException("LDAP proper set must have PROVIDER_URL, SECURITY_PRINCIPAL, SECURITY_CREDENTIALS, USER_BASE_DN, USER_RDN, USER_USERID.");
        }
        this.PROVIDER_URL = (String)tempProps.get("PROVIDER_URL");
        this.SECURITY_PRINCIPAL = (String)tempProps.get("SECURITY_PRINCIPAL");
        this.SECURITY_CREDENTIALS = (String)tempProps.get("SECURITY_CREDENTIALS");
        if (Utility.isValueEncryptionFlagExists("SECURITY_CREDENTIALS", tempProps)) {
            this.SECURITY_CREDENTIALS = Password.getPlainStatic(this.SECURITY_CREDENTIALS);
        }
        this.USER_BASE_DN = (String)tempProps.get("USER_BASE_DN");
        this.USER_RDN = (String)tempProps.get("USER_RDN");
        this.USER_USERID = (String)tempProps.get("USER_USERID");
        if (tempProps.get("INITIAL_CONTEXT_FACTORY") != null) {
            this.INITIAL_CONTEXT_FACTORY = (String)tempProps.get("INITIAL_CONTEXT_FACTORY");
        }
        if (tempProps.get("USER_FIRSTNAME") != null) {
            this.USER_FIRSTNAME = (String)tempProps.get("USER_FIRSTNAME");
        }
        if (tempProps.get("USER_LASTNAME") != null) {
            this.USER_LASTNAME = (String)tempProps.get("USER_LASTNAME");
        }
        if (tempProps.get("USER_MAINEMAIL") != null) {
            this.USER_MAINEMAIL = (String)tempProps.get("USER_MAINEMAIL");
        }
    }
    
    @Override
    public void connect() throws NamingException {
        (this.env = new Hashtable<String, Object>()).put("java.naming.factory.initial", this.INITIAL_CONTEXT_FACTORY);
        this.env.put("java.naming.provider.url", this.PROVIDER_URL);
        this.env.put("java.naming.security.authentication", this.SECURITY_AUTHENTICATION);
        this.env.put("java.naming.security.principal", this.SECURITY_PRINCIPAL);
        this.env.put("java.naming.security.credentials", this.SECURITY_CREDENTIALS);
        this.ctx = new InitialDirContext(this.env);
    }
    
    @Override
    public boolean getAccess(final String uid, final Password passwd) {
        boolean result = false;
        if (uid == null || passwd == null) {
            return result;
        }
        final String dn = this.USER_RDN + "=" + uid + "," + this.USER_BASE_DN;
        final Hashtable<String, Object> envTemp = new Hashtable<String, Object>();
        envTemp.put("java.naming.factory.initial", this.INITIAL_CONTEXT_FACTORY);
        envTemp.put("java.naming.provider.url", this.PROVIDER_URL);
        envTemp.put("java.naming.security.authentication", this.SECURITY_AUTHENTICATION);
        envTemp.put("java.naming.security.principal", dn);
        envTemp.put("java.naming.security.credentials", passwd.getPlain());
        DirContext dc = null;
        try {
            dc = new InitialDirContext(envTemp);
            result = (dc != null);
        }
        catch (NamingException e) {
            LDAPAuthLayer.logger.warn((Object)"UserId or Password is incorrect");
        }
        finally {
            if (dc != null) {
                try {
                    dc.close();
                }
                catch (NamingException ex) {}
            }
        }
        return result;
    }
    
    @Override
    public AuthType getType() {
        return AuthType.LDAP;
    }
    
    @Override
    public boolean find(final String uid) {
        boolean found = false;
        try {
            if (this.ctx == null) {
                this.connect();
            }
            final SearchControls controls = new SearchControls();
            controls.setSearchScope(2);
            final String[] showAttris = { "cn", "sn", "uid", this.USER_USERID };
            controls.setReturningAttributes(showAttris);
            final String filter = "(" + this.USER_USERID + "=" + uid + ")";
            if (LDAPAuthLayer.logger.isDebugEnabled()) {
                LDAPAuthLayer.logger.debug((Object)("searching for user:" + uid + ". DN:" + this.USER_BASE_DN + ", filter:" + filter));
            }
            final NamingEnumeration results = this.ctx.search(this.USER_BASE_DN, filter, controls);
            while (results.hasMore()) {
                final SearchResult searchResult = (SearchResult)results.next();
                final Attributes attributes = searchResult.getAttributes();
                if (LDAPAuthLayer.logger.isTraceEnabled()) {
                    LDAPAuthLayer.logger.trace((Object)("All attributes = " + attributes));
                }
                final Attribute attr = attributes.get(this.USER_USERID);
                if (attr != null && uid.equalsIgnoreCase((String)attr.get())) {
                    found = true;
                    break;
                }
            }
        }
        catch (NamingException ne) {
            throw new DatallRuntimeException((IError)Error.INVALID_LDAP_CONFIGURATION, (Throwable)ne, "LDAP", new String[0]);
        }
        return found;
    }
    
    @Override
    public void close() {
        if (this.ctx != null) {
            try {
                this.ctx.close();
            }
            catch (NamingException e) {
                LDAPAuthLayer.logger.error((Object)"error closing LDAP context.", (Throwable)e);
            }
        }
    }
    
    static {
        LDAPAuthLayer.logger = Logger.getLogger((Class)LDAPAuthLayer.class);
        LDAPAuthLayer.INSTANCES = new HashMap<String, LDAPAuthLayer>();
    }
}
