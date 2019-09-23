package com.datasphere.upgrades;

import com.datasphere.metaRepository.*;

public interface Upgrade
{
    void upgrade(final String p0, final String p1) throws MetaDataRepositoryException;
}
