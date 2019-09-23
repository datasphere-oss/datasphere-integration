package com.datasphere.rest.apiv2.appmgmt.models;

import java.util.*;

public class Template
{
    private String templateName;
    private List<TemplateParameter> sourceParameters;
    private List<TemplateParameter> targetParameters;
    private List<TemplateParameter> parserParameters;
    private List<TemplateParameter> formatterParameters;
    
    public Template() {
        this.templateName = null;
        this.sourceParameters = new ArrayList<TemplateParameter>();
        this.targetParameters = new ArrayList<TemplateParameter>();
        this.parserParameters = new ArrayList<TemplateParameter>();
        this.formatterParameters = new ArrayList<TemplateParameter>();
    }
    
    public Template templateName(final String templateName) {
        this.templateName = templateName;
        return this;
    }
    
    public String getTemplateName() {
        return this.templateName;
    }
    
    public void setTemplateName(final String templateName) {
        this.templateName = templateName;
    }
    
    public Template sourceParameters(final List<TemplateParameter> sourceParameters) {
        this.sourceParameters = sourceParameters;
        return this;
    }
    
    public List<TemplateParameter> getSourceParameters() {
        return this.sourceParameters;
    }
    
    public void setSourceParameters(final List<TemplateParameter> sourceParameters) {
        this.sourceParameters = sourceParameters;
    }
    
    public Template targetParameters(final List<TemplateParameter> targetParameters) {
        this.targetParameters = targetParameters;
        return this;
    }
    
    public List<TemplateParameter> getTargetParameters() {
        return this.targetParameters;
    }
    
    public void setTargetParameters(final List<TemplateParameter> targetParameters) {
        this.targetParameters = targetParameters;
    }
    
    public Template parserParameters(final List<TemplateParameter> parserParameters) {
        this.parserParameters = parserParameters;
        return this;
    }
    
    public List<TemplateParameter> getParserParameters() {
        return this.parserParameters;
    }
    
    public void setParserParameters(final List<TemplateParameter> parserParameters) {
        this.parserParameters = parserParameters;
    }
    
    public Template formatterParameters(final List<TemplateParameter> formatterParameters) {
        this.formatterParameters = formatterParameters;
        return this;
    }
    
    public List<TemplateParameter> getFormatterParameters() {
        return this.formatterParameters;
    }
    
    public void setFormatterParameters(final List<TemplateParameter> formatterParameters) {
        this.formatterParameters = formatterParameters;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final Template template = (Template)o;
        return Objects.equals(this.templateName, template.templateName) && Objects.equals(this.sourceParameters, template.sourceParameters) && Objects.equals(this.targetParameters, template.targetParameters);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.templateName, this.sourceParameters, this.targetParameters);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class Template {\n");
        sb.append("    templateName: ").append(this.toIndentedString(this.templateName)).append("\n");
        sb.append("    sourceParameters: ").append(this.toIndentedString(this.sourceParameters)).append("\n");
        sb.append("    targetParameters: ").append(this.toIndentedString(this.targetParameters)).append("\n");
        sb.append("}");
        return sb.toString();
    }
    
    private String toIndentedString(final Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}
