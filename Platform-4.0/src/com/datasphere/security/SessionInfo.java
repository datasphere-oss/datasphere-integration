package com.datasphere.security;

import java.io.*;

public class SessionInfo implements Serializable
{
    private static final long serialVersionUID = -390069817026389669L;
    public final String userId;
    public final long loginTime;
    public final String clientId;
    public final String type;
    
    private SessionInfo() {
        this(null, 0L, null, "");
    }
    
    public SessionInfo(final String userId, final long loginTime) {
        this(userId, loginTime, null, "");
    }
    
    public SessionInfo(final String userId, final long loginTime, final String clientId, final String type) {
        this.userId = userId;
        this.loginTime = loginTime;
        this.clientId = clientId;
        this.type = ((type == null) ? "" : type);
    }
}
