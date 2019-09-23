package com.datasphere.security;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.exception.SecurityException;
import com.fasterxml.jackson.annotation.JsonIgnore;

import flexjson.JSON;

public class Password implements Serializable
{
    private static final long serialVersionUID = -3853998113238328851L;
    private static Logger logger;
    private transient byte[] salt;
    @JsonIgnore
    @JSON(include = false)
    private String plain;
    private String encrypted;
    
    public Password() {
        this.salt = null;
        try {
            this.salt = Arrays.copyOf("gunturchilli".getBytes("UTF8"), 8);
        }
        catch (Exception ex) {}
    }
    
    public Password(final String plain) {
        this.salt = null;
        try {
            this.salt = Arrays.copyOf("gunturchilli".getBytes("UTF8"), 8);
            this.setPlain(plain);
        }
        catch (Exception ex) {}
    }
    
    public String getPlain() {
        return this.plain;
    }
    
    public void setPlain(final String plain) {
        this.plain = plain;
        try {
            this.encrypted = HSecurityManager.encrypt(plain, this.salt);
        }
        catch (Exception ex) {
            Password.logger.error((Object)"exception encrypting ", (Throwable)ex);
        }
    }
    
    public String getEncrypted() {
        return this.encrypted;
    }
    
    public void setEncrypted(final String encrypted) throws SecurityException {
        this.encrypted = encrypted;
        try {
            this.plain = HSecurityManager.decrypt(encrypted, this.salt);
        }
        catch (Exception ex) {
            throw new SecurityException("Exception decrypting because " + ex.getMessage());
        }
    }
    
    public static String getEncryptedStatic(final String plain) {
        if (plain == null || plain.isEmpty()) {
            return plain;
        }
        String enc = null;
        try {
            enc = HSecurityManager.encrypt(plain, SaltClass.getSalt());
        }
        catch (Exception ex) {
            Password.logger.error((Object)"exception encrypting ", (Throwable)ex);
        }
        return enc;
    }
    
    public static String getPlainStatic(final String enc) {
        if (enc == null || enc.isEmpty()) {
            return enc;
        }
        String pln = null;
        try {
            pln = HSecurityManager.decrypt(enc, SaltClass.getSalt());
        }
        catch (Exception ex) {
            Password.logger.error((Object)"exception decrypting ", (Throwable)ex);
        }
        return pln;
    }
    
    @Override
    public String toString() {
        return this.encrypted;
    }
    
    public static String extractPassword(final Object obj) throws Exception {
        String res = null;
        if (obj == null) {
            return res;
        }
        res = extractPaswordforUpgrade(obj);
        if (res != null) {
            return res;
        }
        if (obj instanceof Password) {
            res = ((Password)obj).getPlain();
        }
        else if (obj instanceof Map) {
            final String enp = (String)((Map)obj).get("encrypted");
            final Password pp1 = new Password();
            pp1.setEncrypted(enp);
            res = pp1.getPlain();
        }
        else {
            if (!(obj instanceof String)) {
                throw new Exception("unknown value provided for Password field in properties");
            }
            res = (String)obj;
        }
        return res;
    }
    
    public static String extractPaswordforUpgrade(final Object obj) throws Exception {
        if (obj instanceof Password) {
            String pp = ((Password)obj).toString();
            if (pp.startsWith("{") && pp.endsWith("}")) {
                final String[] parts = pp.split("\"");
                if (parts != null && parts.length > 3) {
                    final Password pp2 = new Password();
                    pp2.setEncrypted(parts[3]);
                    return pp2.getPlain();
                }
                final String[] parts2 = pp.split("encrypted=");
                if (parts2 != null && parts2.length > 1) {
                    final String str2 = parts2[1];
                    final String str3 = str2.substring(0, str2.length() - 2);
                    final Password pp3 = new Password();
                    pp3.setEncrypted(str3);
                    return pp3.getPlain();
                }
            }
            pp = ((Password)obj).getPlain();
            if (pp.startsWith("{") && pp.endsWith("}")) {
                final String[] parts = pp.split("\"");
                if (parts != null && parts.length > 3) {
                    final Password pp2 = new Password();
                    pp2.setEncrypted(parts[3]);
                    return pp2.getPlain();
                }
            }
            final String[] parts3 = pp.split("encrypted=");
            if (parts3 != null && parts3.length > 1) {
                final String str4 = parts3[1];
                final String str5 = str4.substring(0, str4.length() - 2);
                final Password pp4 = new Password();
                pp4.setEncrypted(str5);
                return pp4.getPlain();
            }
        }
        return null;
    }
    
    static {
        Password.logger = Logger.getLogger((Class)Password.class);
    }
    
    private static class SaltClass
    {
        byte[] salt;
        static SaltClass instance;
        
        private SaltClass() {
            try {
                this.salt = Arrays.copyOf("gunturchilli".getBytes("UTF8"), 8);
            }
            catch (Exception ex) {
                this.salt = null;
            }
        }
        
        public static synchronized byte[] getSalt() {
            if (SaltClass.instance == null) {
                SaltClass.instance = new SaltClass();
            }
            return SaltClass.instance.salt;
        }
    }
}
