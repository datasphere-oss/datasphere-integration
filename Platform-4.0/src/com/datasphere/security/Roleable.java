package com.datasphere.security;

import com.datasphere.runtime.meta.*;
import java.util.*;
import com.datasphere.metaRepository.*;

public interface Roleable
{
    void grantRole(final MetaInfo.Role p0);
    
    void grantRoles(final List<MetaInfo.Role> p0);
    
    void revokeRole(final MetaInfo.Role p0);
    
    void revokeRoles(final List<MetaInfo.Role> p0);
    
    List<MetaInfo.Role> getRoles() throws MetaDataRepositoryException;
    
    void setRoles(final List<MetaInfo.Role> p0);
    
    List<ObjectPermission> getAllPermissions() throws MetaDataRepositoryException;
}
