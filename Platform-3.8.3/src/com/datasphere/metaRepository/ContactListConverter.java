package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;
import com.datasphere.runtime.meta.*;

public class ContactListConverter extends PropertyDefConverter
{
    private static final long serialVersionUID = -5126868144838596329L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<MetaInfo.ContactMechanism>>() {};
    }
}
