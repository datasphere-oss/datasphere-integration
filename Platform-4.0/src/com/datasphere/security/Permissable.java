package com.datasphere.security;

import java.util.List;

import com.datasphere.exception.SecurityException;
import com.datasphere.metaRepository.MetaDataRepositoryException;

public interface Permissable
{
    void grantPermission(final ObjectPermission p0) throws SecurityException, MetaDataRepositoryException;
    
    void grantPermissions(final List<ObjectPermission> p0) throws SecurityException, MetaDataRepositoryException;
    
    void revokePermission(final ObjectPermission p0) throws SecurityException, MetaDataRepositoryException;
    
    void revokePermissions(final List<ObjectPermission> p0) throws SecurityException, MetaDataRepositoryException;
    
    List<ObjectPermission> getPermissions();
}
