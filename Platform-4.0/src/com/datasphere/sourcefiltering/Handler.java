package com.datasphere.sourcefiltering;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.stmts.CreateSourceOrTargetStmt;

interface Handler
{
    Object handle(final Compiler p0, final CreateSourceOrTargetStmt p1) throws MetaDataRepositoryException;
}
