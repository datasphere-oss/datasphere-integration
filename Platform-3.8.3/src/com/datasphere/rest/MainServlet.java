package com.datasphere.rest;

import com.datasphere.exception.*;
import com.datasphere.uuid.*;
import java.util.*;

import javax.servlet.http.*;

public class MainServlet extends HttpServlet
{
    protected AuthToken getToken(final HttpServletRequest request) {
        AuthToken token = null;
        String rawToken = null;
        final String authorizationHeader = request.getHeader("Authorization");
        if (authorizationHeader != null) {
            final StringTokenizer tokenizer = new StringTokenizer(authorizationHeader);
            if (tokenizer.countTokens() > 1) {
                final String authscheme = tokenizer.nextToken();
                rawToken = tokenizer.nextToken();
            }
        }
        if (rawToken == null) {
            rawToken = request.getParameter("token");
        }
        if (rawToken == null) {
            final Cookie[] cookies2;
            final Cookie[] cookies = cookies2 = request.getCookies();
            for (final Cookie cookie : cookies2) {
                if (cookie.getName().equals("token")) {
                    rawToken = cookie.getValue();
                }
            }
        }
        try {
            if (rawToken == null) {
                throw new InvalidUriException("The URI is invalid, please provide an authentication token with the request\nTo get an authentication token do a POST Request as curl -X POST -d'username=HD user name&password=HD password' http://IP address of a HD Server:port/security/authenticate from terminal");
            }
            token = new AuthToken(rawToken);
        }
        catch (ExpiredSessionException ex) {}
        return token;
    }
}
