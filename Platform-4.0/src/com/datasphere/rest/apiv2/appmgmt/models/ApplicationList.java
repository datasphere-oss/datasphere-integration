package com.datasphere.rest.apiv2.appmgmt.models;

import java.util.*;

public class ApplicationList extends ArrayList<Application>
{
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final ApplicationList applicationList = (ApplicationList)o;
        return true;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(new Object[0]);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class ApplicationList {\n");
        sb.append("    ").append(this.toIndentedString(super.toString())).append("\n");
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
