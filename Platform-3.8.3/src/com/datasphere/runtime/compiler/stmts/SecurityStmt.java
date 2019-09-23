package com.datasphere.runtime.compiler.stmts;

import java.util.*;
import com.datasphere.security.*;

public interface SecurityStmt
{
    String getName();
    
    List<ObjectPermission.Action> getListOfPrivilege();
    
    List<ObjectPermission.ObjectType> getObjectType();
}
