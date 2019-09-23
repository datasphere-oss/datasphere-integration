package com.datasphere.runtime.compiler.stmts;

import java.util.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.meta.*;

public class UserProperty
{
    public List<String> lroles;
    String uname;
    public String defaultnamespace;
    public List<Property> properties;
    public MetaInfo.User.AUTHORIZATION_TYPE originType;
    public String ldap;
    private String alias;
    
    public UserProperty(final List<String> lroles, final String defaultnamespace, final List<Property> props) {
        this.originType = MetaInfo.User.AUTHORIZATION_TYPE.INTERNAL;
        this.lroles = lroles;
        this.defaultnamespace = defaultnamespace;
        this.properties = props;
    }
    
    public String getAlias() {
        return this.alias;
    }
    
    public void setAlias(final String alias) {
        this.alias = alias;
    }
    
    public void setUname(final String uname) {
        this.uname = uname;
    }
}
