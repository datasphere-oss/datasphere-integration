package com.datasphere.jmqmessaging;

import java.io.*;
import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;
import org.apache.commons.lang.builder.*;

public class AuthenticationRequest implements KryoSerializable, Serializable
{
    private static final long serialVersionUID = -8399218291499574416L;
    private String username;
    private String password;
    
    public AuthenticationRequest() {
    }
    
    public AuthenticationRequest(final String user, final String pwd) {
        this.username = user;
        this.password = pwd;
    }
    
    public String getUsername() {
        return this.username;
    }
    
    public String getPassword() {
        return this.password;
    }
    
    public void write(final Kryo kryo, final Output output) {
        if (this.username != null) {
            output.writeByte(0);
            output.writeString(this.username);
        }
        else {
            output.writeByte(1);
        }
        if (this.password != null) {
            output.writeByte(0);
            output.writeString(this.password);
        }
        else {
            output.writeByte(1);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        final byte hasUsername = input.readByte();
        if (hasUsername == 0) {
            this.username = input.readString();
        }
        else {
            this.username = null;
        }
        final byte hasPassword = input.readByte();
        if (hasPassword == 0) {
            this.password = input.readString();
        }
        else {
            this.password = null;
        }
    }
    
    @Override
    public String toString() {
        return "AuthenticationRequest(" + this.username + ":" + this.password + ")";
    }
    
    @Override
    public boolean equals(final Object o) {
        if (o instanceof AuthenticationRequest) {
            final AuthenticationRequest req = (AuthenticationRequest)o;
            return this.username.equals(req.getUsername()) && this.password.equals(req.getPassword());
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.username).append((Object)this.password).toHashCode();
    }
}
