package com.datasphere.rest.apiv2.integration.stencils;

import com.datasphere.uuid.*;
import com.datasphere.metaRepository.*;
import com.datasphere.rest.apiv2.integration.models.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.components.*;

public abstract class BaseStencil implements IStencil
{
    @Override
    public abstract Stencil getStencilModel(final AuthToken p0) throws MetaDataRepositoryException;
    
    @Override
    public void createTopology(final AuthToken token, final TopologyRequest topologyRequest) throws MetaDataRepositoryException {
        final QueryValidator queryValidator = new QueryValidator(Server.getServer());
        queryValidator.createNewContext(token);
        queryValidator.setUpdateMode(true);
        queryValidator.CreateNameSpaceStatement(token, topologyRequest.getTopologyName(), false);
    }
    
    @Override
    public abstract void startTopology(final AuthToken p0, final String p1) throws MetaDataRepositoryException;
    
    @Override
    public abstract void stopTopology(final AuthToken p0, final String p1) throws MetaDataRepositoryException;
    
    @Override
    public void dropTopology(final AuthToken token, final String topologyName) throws MetaDataRepositoryException {
        final QueryValidator queryValidator = new QueryValidator(Server.getServer());
        queryValidator.createNewContext(token);
        queryValidator.setUpdateMode(true);
        queryValidator.DropStatement(token, EntityType.NAMESPACE.name(), topologyName, true, true);
    }
}
