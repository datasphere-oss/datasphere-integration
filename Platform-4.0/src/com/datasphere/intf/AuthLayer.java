package com.datasphere.intf;

import java.util.*;
import javax.naming.*;
import com.datasphere.security.*;

public interface AuthLayer
{
    void setup(final Map<String, Object> p0);
    
    void connect() throws NamingException;
    
    boolean find(final String p0);
    
    boolean getAccess(final String p0, final Password p1);
    
    AuthType getType();
    
    void close();
    
    public enum AuthType
    {
        LDAP, 
        AD;
    }
}
