package com.datasphere.uuid;

public class AuthToken extends UUID
{
    private static final long serialVersionUID = 4392763777100817312L;
    
    public AuthToken() {
        this(System.currentTimeMillis());
    }
    
    public AuthToken(final long currentTimeMillis) {
        super(currentTimeMillis);
    }
    
    public AuthToken(final long time, final long clockSeqAndNode) {
        super(time, clockSeqAndNode);
    }
    
    public AuthToken(final UUID u) {
        super(u);
    }
    
    public AuthToken(final String uidstr) {
        super(uidstr);
    }
    
    public AuthToken(final CharSequence s) {
        super(s);
    }
}
