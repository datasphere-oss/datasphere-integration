package com.datasphere.wizard;

import java.util.*;

import com.datasphere.Connection.*;
import com.datasphere.exception.*;

public class DatabaseWriterWizard implements TargetWizard
{
    public static final String DB_URL = "ConnectionURL";
    public static final String DB_USER = "Username";
    public static final String DB_PASSWORD = "Password";
    
    public String validateConnection(final Map<String, Object> request) {
        final ValidationResult result = new ValidationResult();
        String dbUrl = "";
        String dbUser = "";
        String dbPasswd = "";
        try {
            WizardUtil.validateRequest(new String[] { "ConnectionURL", "Username", "Password" }, (Map)request);
            dbUrl = request.get("ConnectionURL").toString();
            dbUser = request.get("Username").toString();
            dbPasswd = request.get("Password").toString();
            final DatabaseWriterConnection connection = DatabaseWriterConnection.getConnection(dbUrl, dbUser, dbPasswd);
            connection.disconnect();
            result.result = true;
            result.message = "Connection Successful!!!";
            return result.toJson();
        }
        catch (IllegalArgumentException ex) {
            result.result = false;
            result.message = ex.getMessage();
            return result.toJson();
        }
        catch (DatabaseWriterException e) {
            result.result = false;
            String msg = e.getMessage().trim();
            if (msg.startsWith("2704 :")) {
                msg = "Invalid ConnectionURL format";
            }
            if (msg.startsWith("2706 :")) {
                msg = "Invalid Username format";
            }
            if (msg.startsWith("2708 :")) {
                msg = "Invalid Password format";
            }
            if (msg.startsWith("2711 :")) {
                msg = "Failure in connecting to Database.   with url : " + dbUrl + " username : " + dbUser;
            }
            result.message = msg;
            return result.toJson();
        }
    }
    
    public String list(final Map<String, Object> request) {
        final ValidationResult result = new ValidationResult();
        result.result = false;
        result.message = "Operation not supported!!!";
        return result.toJson();
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
