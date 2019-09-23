package com.datasphere.rest.apiv2.appmgmt.models;

import java.util.*;

public class ApplicationParametersApplicationSettingsRecovery
{
    private Boolean enabled;
    private Integer period;
    
    public ApplicationParametersApplicationSettingsRecovery() {
        this.enabled = null;
        this.period = null;
    }
    
    public ApplicationParametersApplicationSettingsRecovery enabled(final Boolean enabled) {
        this.enabled = enabled;
        return this;
    }
    
    public Boolean getEnabled() {
        return this.enabled;
    }
    
    public void setEnabled(final Boolean enabled) {
        this.enabled = enabled;
    }
    
    public ApplicationParametersApplicationSettingsRecovery period(final Integer period) {
        this.period = period;
        return this;
    }
    
    public Integer getPeriod() {
        return this.period;
    }
    
    public void setPeriod(final Integer period) {
        this.period = period;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final ApplicationParametersApplicationSettingsRecovery applicationParametersApplicationSettingsRecovery = (ApplicationParametersApplicationSettingsRecovery)o;
        return Objects.equals(this.enabled, applicationParametersApplicationSettingsRecovery.enabled) && Objects.equals(this.period, applicationParametersApplicationSettingsRecovery.period);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.enabled, this.period);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class ApplicationParametersApplicationSettingsRecovery {\n");
        sb.append("    enabled: ").append(this.toIndentedString(this.enabled)).append("\n");
        sb.append("    period: ").append(this.toIndentedString(this.period)).append("\n");
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
