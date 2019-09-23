package com.datasphere.jmqmessaging;

import java.io.*;
import com.datasphere.uuid.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import org.apache.commons.lang.builder.*;

public class AuthenticationResponse implements KryoSerializable, Serializable
{
    private static final long serialVersionUID = -6711064046717166349L;
    private boolean isAuthenticated;
    private AuthToken authToken;
    
    public AuthenticationResponse() {
        this.isAuthenticated = false;
    }
    
    public AuthenticationResponse(final AuthToken token) {
        this.authToken = token;
        if (token != null) {
            this.isAuthenticated = true;
        }
        else {
            this.isAuthenticated = false;
        }
    }
    
    public AuthToken getAuthToken() {
        return this.authToken;
    }
    
    public boolean isAuthenticated() {
        return this.isAuthenticated;
    }
    
    public void write(final Kryo kryo, final Output output) {
        if (this.isAuthenticated) {
            output.writeBoolean(true);
            this.authToken.write(kryo, output);
        }
        else {
            output.writeBoolean(false);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        this.isAuthenticated = input.readBoolean();
        if (this.isAuthenticated) {
            if (this.authToken == null) {
                this.authToken = new AuthToken();
            }
            this.authToken.read(kryo, input);
        }
    }
    
    @Override
    public String toString() {
        return "AuthenticationResponse(" + (this.isAuthenticated ? this.authToken.toString() : "NonAuthenticated") + ")";
    }
    
    @Override
    public boolean equals(final Object o) {
        if (o instanceof AuthenticationResponse) {
            final AuthenticationResponse res = (AuthenticationResponse)o;
            final boolean ret = this.isAuthenticated == res.isAuthenticated();
            return !ret || !this.isAuthenticated || this.authToken.equals((Object)res.getAuthToken());
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.isAuthenticated).append((Object)this.authToken.toString()).toHashCode();
    }
}
