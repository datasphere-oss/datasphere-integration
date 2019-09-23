package com.datasphere.rest.apiv2.appmgmt.models;

import java.util.*;

public class TemplateParameter
{
    private String name;
    private ParameterTypeEnum parameterType;
    private Boolean required;
    
    public TemplateParameter() {
        this.name = null;
        this.parameterType = null;
        this.required = null;
    }
    
    public TemplateParameter name(final String name) {
        this.name = name;
        return this;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public TemplateParameter parameterType(final ParameterTypeEnum parameterType) {
        this.parameterType = parameterType;
        return this;
    }
    
    public ParameterTypeEnum getParameterType() {
        return this.parameterType;
    }
    
    public void setParameterType(final ParameterTypeEnum parameterType) {
        this.parameterType = parameterType;
    }
    
    public TemplateParameter required(final Boolean required) {
        this.required = required;
        return this;
    }
    
    public Boolean getRequired() {
        return this.required;
    }
    
    public void setRequired(final Boolean required) {
        this.required = required;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final TemplateParameter templateParameter = (TemplateParameter)o;
        return Objects.equals(this.name, templateParameter.name) && Objects.equals(this.parameterType, templateParameter.parameterType) && Objects.equals(this.required, templateParameter.required);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.parameterType, this.required);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class TemplateParameter {\n");
        sb.append("    name: ").append(this.toIndentedString(this.name)).append("\n");
        sb.append("    parameterType: ").append(this.toIndentedString(this.parameterType)).append("\n");
        sb.append("    required: ").append(this.toIndentedString(this.required)).append("\n");
        sb.append("}");
        return sb.toString();
    }
    
    private String toIndentedString(final Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
    
    public enum ParameterTypeEnum
    {
        STRING, 
        SECURESTRING, 
        INT, 
        BOOL, 
        OBJECT, 
        SECUREOBJECT, 
        ARRAY;
        
        public String value() {
            return this.name();
        }
        
        public static ParameterTypeEnum fromValue(final String v) {
            return valueOf(v);
        }
    }
}
