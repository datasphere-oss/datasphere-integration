package com.datasphere.rest.apiv2.integration.models;

import java.util.*;

public class TopologyList extends ArrayList<Topology>
{
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final TopologyList topologyList = (TopologyList)o;
        return true;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(new Object[0]);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class TopologyList {\n");
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
