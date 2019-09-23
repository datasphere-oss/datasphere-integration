package com.datasphere.rest.apiv2.appmgmt.models;

import java.util.*;

public class ApplicationParameters
{
    private String templateId;
    private String applicationName;
    private ApplicationParametersApplicationSettings applicationSettings;
    private Map<String, String> sourceParameters;
    private Map<String, String> targetParameters;
    private Map<String, String> parserParameters;
    private Map<String, String> formatterParameters;
    
    public ApplicationParameters() {
        this.templateId = null;
        this.applicationName = null;
        this.applicationSettings = null;
        this.sourceParameters = new HashMap<String, String>();
        this.targetParameters = new HashMap<String, String>();
        this.parserParameters = new HashMap<String, String>();
        this.formatterParameters = new HashMap<String, String>();
    }
    
    public ApplicationParameters templateId(final String templateId) {
        this.templateId = templateId;
        return this;
    }
    
    public String getTemplateId() {
        return this.templateId;
    }
    
    public void setTemplateId(final String templateId) {
        this.templateId = templateId;
    }
    
    public ApplicationParameters applicationName(final String applicationName) {
        this.applicationName = applicationName;
        return this;
    }
    
    public String getApplicationName() {
        return this.applicationName;
    }
    
    public void setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
    }
    
    public ApplicationParameters applicationSettings(final ApplicationParametersApplicationSettings applicationSettings) {
        this.applicationSettings = applicationSettings;
        return this;
    }
    
    public ApplicationParametersApplicationSettings getApplicationSettings() {
        return this.applicationSettings;
    }
    
    public void setApplicationSettings(final ApplicationParametersApplicationSettings applicationSettings) {
        this.applicationSettings = applicationSettings;
    }
    
    public ApplicationParameters sourceParameters(final Map<String, String> sourceParameters) {
        this.sourceParameters = sourceParameters;
        return this;
    }
    
    public Map<String, String> getSourceParameters() {
        return this.sourceParameters;
    }
    
    public void setSourceParameters(final Map<String, String> sourceParameters) {
        this.sourceParameters = sourceParameters;
    }
    
    public ApplicationParameters targetParameters(final Map<String, String> targetParameters) {
        this.targetParameters = targetParameters;
        return this;
    }
    
    public Map<String, String> getTargetParameters() {
        return this.targetParameters;
    }
    
    public void setTargetParameters(final Map<String, String> targetParameters) {
        this.targetParameters = targetParameters;
    }
    
    public ApplicationParameters parserParameters(final Map<String, String> parserParameters) {
        this.parserParameters = parserParameters;
        return this;
    }
    
    public Map<String, String> getParserParameters() {
        return this.parserParameters;
    }
    
    public void setParserParameters(final Map<String, String> parserParameters) {
        this.parserParameters = parserParameters;
    }
    
    public ApplicationParameters formatterParameters(final Map<String, String> formatterParameters) {
        this.formatterParameters = formatterParameters;
        return this;
    }
    
    public Map<String, String> getFormatterParameters() {
        return this.formatterParameters;
    }
    
    public void setFormatterParameters(final Map<String, String> formatterParameters) {
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
        final ApplicationParameters applicationParameters = (ApplicationParameters)o;
        return Objects.equals(this.templateId, applicationParameters.templateId) && Objects.equals(this.applicationName, applicationParameters.applicationName) && Objects.equals(this.applicationSettings, applicationParameters.applicationSettings) && Objects.equals(this.sourceParameters, applicationParameters.sourceParameters) && Objects.equals(this.targetParameters, applicationParameters.targetParameters) && Objects.equals(this.parserParameters, applicationParameters.parserParameters) && Objects.equals(this.formatterParameters, applicationParameters.formatterParameters);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.templateId, this.applicationName, this.applicationSettings, this.sourceParameters, this.targetParameters, this.parserParameters, this.formatterParameters);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class ApplicationParameters {\n");
        sb.append("    templateId: ").append(this.toIndentedString(this.templateId)).append("\n");
        sb.append("    applicationName: ").append(this.toIndentedString(this.applicationName)).append("\n");
        sb.append("    applicationSettings: ").append(this.toIndentedString(this.applicationSettings)).append("\n");
        sb.append("    sourceParameters: ").append(this.toIndentedString(this.sourceParameters)).append("\n");
        sb.append("    targetParameters: ").append(this.toIndentedString(this.targetParameters)).append("\n");
        sb.append("    parserParameters: ").append(this.toIndentedString(this.parserParameters)).append("\n");
        sb.append("    formatterParameters: ").append(this.toIndentedString(this.formatterParameters)).append("\n");
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
