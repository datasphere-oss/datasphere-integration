package com.datasphere.runtime.components;

import com.datasphere.uuid.*;
import com.datasphere.metaRepository.*;

public interface MetaObjectPermissionChecker
{
    boolean checkPermissionForMetaPropertyVariable(final AuthToken p0) throws MetaDataRepositoryException;
}
