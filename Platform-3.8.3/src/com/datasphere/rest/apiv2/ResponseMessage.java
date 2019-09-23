package com.datasphere.rest.apiv2;

import java.util.*;

public class ResponseMessage
{
    private String message;
    
    public ResponseMessage() {
        this.message = null;
    }
    
    public ResponseMessage message(final String message) {
        this.message = message;
        return this;
    }
    
    public String getMessage() {
        return this.message;
    }
    
    public void setMessage(final String message) {
        this.message = message;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final ResponseMessage other = (ResponseMessage)o;
        return Objects.equals(this.message, other.message);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.message);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class ResponseMessage {\n");
        sb.append("    message: ").append(this.toIndentedString(this.message)).append("\n");
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
