package com.datasphere.sourcefiltering;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.stmts.CreateSourceOrTargetStmt;

interface Manager
{
    Object receive(final Handler p0, final Compiler p1, final CreateSourceOrTargetStmt p2) throws MetaDataRepositoryException;
}
