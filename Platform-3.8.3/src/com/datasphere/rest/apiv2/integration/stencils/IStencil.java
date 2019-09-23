package com.datasphere.rest.apiv2.integration.stencils;

import com.datasphere.uuid.*;
import com.datasphere.metaRepository.*;
import com.datasphere.rest.apiv2.integration.models.*;

public interface IStencil
{
    public static final String STENCIL_PROPERTYSET_NAME = "StencilProperties";
    public static final String STENCIL_PROPERTYSET_KEY_NAME = "StencilName";
    
    Stencil getStencilModel(final AuthToken p0) throws MetaDataRepositoryException;
    
    void createTopology(final AuthToken p0, final TopologyRequest p1) throws MetaDataRepositoryException;
    
    void startTopology(final AuthToken p0, final String p1) throws MetaDataRepositoryException;
    
    void stopTopology(final AuthToken p0, final String p1) throws MetaDataRepositoryException;
    
    void dropTopology(final AuthToken p0, final String p1) throws MetaDataRepositoryException;
}
