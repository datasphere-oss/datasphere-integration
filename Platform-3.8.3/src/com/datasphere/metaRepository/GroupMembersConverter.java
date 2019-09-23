package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;

public class GroupMembersConverter extends PropertyDefConverter
{
    private static final long serialVersionUID = -5369700228207899772L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<String>>() {};
    }
}
