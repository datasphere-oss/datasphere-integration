package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;
import com.datasphere.security.*;

public class PermissionsListConverter extends PropertyDefConverter
{
    private static final long serialVersionUID = 4550360623119070790L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<ObjectPermission>>() {};
    }
}
