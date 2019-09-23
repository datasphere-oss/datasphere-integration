package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.tungsten.Tungsten;

public class QuitStmt extends Stmt
{
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        if (HazelcastSingleton.isClientMember()) {
            Tungsten.quit = true;
        }
        return null;
    }
}
