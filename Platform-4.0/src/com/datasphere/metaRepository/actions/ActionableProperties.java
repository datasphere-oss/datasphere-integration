package com.datasphere.metaRepository.actions;

import java.io.*;
import com.fasterxml.jackson.databind.node.*;
import com.datasphere.event.*;
import com.fasterxml.jackson.databind.*;

public abstract class ActionableProperties implements Serializable
{
    private ActionableFieldType actionableFieldType;
    private boolean isRequired;
    
    public ActionableFieldType getActionablefieldType() {
        return this.actionableFieldType;
    }
    
    public void setActionableFieldType(final ActionableFieldType actionableFieldType) {
        this.actionableFieldType = actionableFieldType;
    }
    
    public boolean isRequired() {
        return this.isRequired;
    }
    
    public void setIsRequired(final boolean isRequired) {
        this.isRequired = isRequired;
    }
    
    public ObjectNode getJsonObject() {
        final ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
        final ObjectNode node = jsonMapper.createObjectNode();
        node.put("actionAbleFieldType", this.actionableFieldType.name());
        node.put("isRequired", this.isRequired);
        return node;
    }
    
    public enum ActionableFieldType
    {
        TEXT, 
        NUMBER, 
        METAOBJECT, 
        BOOLEAN, 
        ENUM, 
        OBJECT;
    }
}
