package com.datasphere.rest.apiv2;

import java.util.StringTokenizer;

import javax.servlet.http.HttpServletRequest;

import com.datasphere.exception.SecurityException;
import com.datasphere.uuid.AuthToken;

public class Utility
{
    public static AuthToken getAuthTokenFromAuthorizationHeader(final HttpServletRequest request) {
        String rawToken = null;
        final String authorizationHeader = request.getHeader("Authorization");
        if (authorizationHeader != null) {
            final StringTokenizer tokenizer = new StringTokenizer(authorizationHeader);
            if (tokenizer.countTokens() > 1) {
                final String authscheme = tokenizer.nextToken();
                rawToken = tokenizer.nextToken();
            }
        }
        try {
            if (rawToken != null) {
                return new AuthToken(rawToken);
            }
            throw new SecurityException("No Token passed");
        }
        catch (StringIndexOutOfBoundsException sei) {
            throw new SecurityException("Illegal Auth token '" + rawToken + "");
        }
    }
}
