package com.datasphere.rest.apiv2.appmgmt.models;

import java.util.*;

public class ApplicationParametersApplicationSettings
{
    private ApplicationParametersApplicationSettingsRecovery recovery;
    private Boolean encryption;
    private Map<String, String> exceptionHandlers;
    
    public ApplicationParametersApplicationSettings() {
        this.recovery = null;
        this.encryption = false;
        this.exceptionHandlers = new HashMap<String, String>();
    }
    
    public ApplicationParametersApplicationSettings recovery(final ApplicationParametersApplicationSettingsRecovery recovery) {
        this.recovery = recovery;
        return this;
    }
    
    public ApplicationParametersApplicationSettingsRecovery getRecovery() {
        return this.recovery;
    }
    
    public void setRecovery(final ApplicationParametersApplicationSettingsRecovery recovery) {
        this.recovery = recovery;
    }
    
    public ApplicationParametersApplicationSettings encryption(final Boolean encryption) {
        this.encryption = encryption;
        return this;
    }
    
    public Boolean getEncryption() {
        return this.encryption;
    }
    
    public void setEncryption(final Boolean encryption) {
        this.encryption = encryption;
    }
    
    public ApplicationParametersApplicationSettings exceptionHandlers(final Map<String, String> exceptionHandlers) {
        this.exceptionHandlers = exceptionHandlers;
        return this;
    }
    
    public Map<String, String> getExceptionHandlers() {
        return this.exceptionHandlers;
    }
    
    public void setExceptionHandlers(final Map<String, String> exceptionHandlers) {
        this.exceptionHandlers = exceptionHandlers;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final ApplicationParametersApplicationSettings applicationParametersApplicationSettings = (ApplicationParametersApplicationSettings)o;
        return Objects.equals(this.recovery, applicationParametersApplicationSettings.recovery) && Objects.equals(this.encryption, applicationParametersApplicationSettings.encryption) && Objects.equals(this.exceptionHandlers, applicationParametersApplicationSettings.exceptionHandlers);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.recovery, this.encryption, this.exceptionHandlers);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class ApplicationParametersApplicationSettings {\n");
        sb.append("    recovery: ").append(this.toIndentedString(this.recovery)).append("\n");
        sb.append("    encryption: ").append(this.toIndentedString(this.encryption)).append("\n");
        sb.append("    exceptionHandlers: ").append(this.toIndentedString(this.exceptionHandlers)).append("\n");
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
