package com.datasphere.rest.apiv2.appmgmt.templates;

import com.datasphere.uuid.*;
import com.datasphere.metaRepository.*;
import com.datasphere.rest.apiv2.appmgmt.models.*;

public interface ApplicationTemplate
{
    Template getApplicationTemplateModel(final AuthToken p0) throws MetaDataRepositoryException;
    
    void createApplication(final AuthToken p0, final ApplicationParameters p1) throws MetaDataRepositoryException;
}
