package com.datasphere.rest;

import com.datasphere.security.*;
import javax.servlet.http.*;
import org.codehaus.jettison.json.*;
import java.util.regex.*;
import com.datasphere.uuid.*;
import javax.servlet.*;
import java.io.*;

public class SecurityManagerServlet extends HttpServlet
{
    private static final long serialVersionUID = 1L;
    private Pattern authenticatePattern;
    private static final String UI_TYPE = "HD UI";
    
    public SecurityManagerServlet() {
        this.authenticatePattern = Pattern.compile("/authenticate");
    }
    
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        final Matcher authenticateMatcher = this.authenticatePattern.matcher(request.getPathInfo());
        if (authenticateMatcher.find()) {
            final String username = request.getParameter("username");
            final String password = request.getParameter("password");
            final String clientId = request.getParameter("clientId");
            if (username == null || password == null) {
                response.setStatus(400);
                return;
            }
            try {
                final AuthToken token = HSecurityManager.get().authenticate(username, password, clientId, "HD UI");
                final Cookie cookie = new Cookie("token", token.getUUIDString());
                response.addCookie(cookie);
                response.setStatus(200);
                response.setContentType("application/json");
                response.setCharacterEncoding("UTF-8");
                final JSONObject jo = new JSONObject();
                jo.put("token", (Object)token.getUUIDString());
                response.getWriter().write(jo.toString());
                return;
            }
            catch (Exception e) {
                response.setStatus(500);
                return;
            }
        }
        response.setStatus(404);
    }
}
