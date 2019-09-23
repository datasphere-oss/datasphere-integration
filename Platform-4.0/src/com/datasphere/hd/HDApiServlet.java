package com.datasphere.hd;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.datasphere.exception.ExpiredSessionException;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.uuid.AuthToken;

public class HDApiServlet extends HttpServlet
{
    private static Logger logger;
    private static final long serialVersionUID = 8628625019674604142L;
    private HDApi wap;
    private static Set<String> predefined;
    
    public HDApiServlet() {
        this.wap = HDApi.get();
    }
    
    public String getHDStoreDefinition(final HttpServletRequest request, final AuthToken token) throws Exception {
        final String storename = request.getParameter("name");
        final List<MetaInfo.Type>[] storeDef = this.wap.getHDStoreDef(storename);
        if (storeDef != null) {
            final StringBuilder def = new StringBuilder();
            def.append("CONTEXT DEFINITION : \n");
            def.append(storeDef[0].get(0).describe(token));
            def.append("\n");
            if (storeDef[1] != null && storeDef[1].size() > 0) {
                def.append("EVENT LIST DEFINITION : \n");
                final List<MetaInfo.Type> eventDefs = storeDef[1];
                for (final MetaInfo.Type type : eventDefs) {
                    def.append(type.describe(token));
                    def.append("\n");
                }
            }
            return def.toString();
        }
        return "No Definition found for : " + storename;
    }
    
    private String getHDs(final HttpServletRequest request, final AuthToken token) throws Exception {
        Object queryResult = null;
        final String storename = request.getParameter("name");
        if (storename == null) {
            throw new NullPointerException("HD Store NULL not allowed");
        }
        final String query = request.getParameter("query");
        if (query == null) {
            final String[] fields = this.fieldParser(request.getParameter("fields"));
            final Map<String, Object> filters = this.filterParser(request.getParameterMap().get("filter"));
            if (HDApiServlet.logger.isDebugEnabled()) {
                HDApiServlet.logger.debug((Object)"INPUT REQUEST PARAMS : ");
                HDApiServlet.logger.debug((Object)("HD Store : " + storename));
                if (fields != null) {
                    HDApiServlet.logger.debug((Object)"Fields : ");
                    for (int ii = 0; ii < fields.length; ++ii) {
                        HDApiServlet.logger.debug((Object)("\t\t" + fields[ii]));
                    }
                }
                if (filters != null) {
                    HDApiServlet.logger.debug((Object)"Filter Criteria : ");
                    HDApiServlet.logger.debug((Object)filters.entrySet());
                }
            }
            queryResult = this.wap.get(storename, null, fields, filters, token);
        }
        else {
            final Map<String, Object> queryParams = this.getParamsForQuery(request.getParameter("params"));
            queryResult = this.wap.executeQuery(storename, query, queryParams, token);
        }
        if (queryResult == null) {
            return null;
        }
        return this.wap.toJSON(queryResult);
    }
    
    public Map<String, Object> getParamsForQuery(final String params) throws IllegalArgumentException {
        Map<String, Object> queryParams = null;
        if (params != null) {
            final String[] split;
            final String[] allParams = split = params.split(",");
            for (final String aParam : split) {
                final String[] param = aParam.split(":");
                if (param.length != 2) {
                    throw new IllegalArgumentException("Expected format is paramName:paramValue, but got " + param[0] + ":" + param[1]);
                }
                queryParams = new HashMap<String, Object>();
                queryParams.put(param[0], param[1]);
            }
        }
        return queryParams;
    }
    
    public Map<String, Object> filterParser(final String[] filter) {
        if (filter != null) {
            Map<String, Object> map = new HashMap<String, Object>();
            final Map<String, Object> ctx = new HashMap<String, Object>();
            final String[] fltrs = filter[0].split(",");
            if (HDApiServlet.logger.isDebugEnabled()) {
                for (int flt = 0; flt < fltrs.length; ++flt) {
                    HDApiServlet.logger.debug((Object)("Filter : " + fltrs[flt]));
                }
            }
            for (final String s : fltrs) {
                final String[] fltr = s.split(":");
                if (fltr.length == 2 && !fltr[1].isEmpty()) {
                    if (HDApiServlet.logger.isDebugEnabled()) {
                        HDApiServlet.logger.debug((Object)(fltr[0] + " : " + fltr[1]));
                    }
                    String lCase = fltr[0].toLowerCase();
                    if (HDApiServlet.predefined.contains(lCase)) {
                        if (lCase.equalsIgnoreCase("orderby")) {
                            lCase = "sortby";
                        }
                        map.put(lCase, fltr[1]);
                    }
                    else {
                        ctx.put(fltr[0], fltr[1]);
                    }
                }
            }
            if (map.isEmpty() && ctx.isEmpty()) {
                map = null;
            }
            else {
                if (!ctx.isEmpty()) {
                    map.put("context", ctx);
                }
                if (HDApiServlet.logger.isDebugEnabled()) {
                    HDApiServlet.logger.debug((Object)"Filter ");
                    HDApiServlet.logger.debug((Object)map);
                }
            }
            return map;
        }
        return null;
    }
    
    public String[] fieldParser(final String fields) {
        if (fields != null) {
            return fields.split(",");
        }
        return null;
    }
    
    private AuthToken getToken(final HttpServletRequest request) {
        String rawToken = request.getParameter("token");
        if (rawToken == null) {
            final Cookie[] cookies = request.getCookies();
            for (int i = 0; i < cookies.length; ++i) {
                if (cookies[i].getName().equals("token")) {
                    rawToken = cookies[i].getValue();
                }
            }
        }
        if (rawToken != null) {
            return new AuthToken(rawToken);
        }
        return null;
    }
    
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        response.addHeader("Access-Control-Allow-Origin", "*");
        try {
            final AuthToken token = this.getToken(request);
            if (token == null) {
                response.setStatus(401);
                return;
            }
            try {
                final String servletPath = request.getPathInfo();
                String resultOfMethodCall = null;
                final String s = servletPath;
                switch (s) {
                    case "/def": {
                        resultOfMethodCall = this.getHDStoreDefinition(request, token);
                        break;
                    }
                    case "/search": {
                        resultOfMethodCall = this.getHDs(request, token);
                        break;
                    }
                }
                if (resultOfMethodCall == null) {
                    response.setStatus(403);
                    return;
                }
                try {
                    response.setStatus(200);
                    response.getWriter().write(resultOfMethodCall);
                }
                catch (Exception ex2) {
                    if (ex2 instanceof ExpiredSessionException) {
                        response.setStatus(403);
                    }
                    else if (ex2 instanceof ServletException) {
                        response.setStatus(400);
                    }
                    else if (ex2 instanceof NullPointerException) {
                        response.setStatus(400);
                    }
                    else if (ex2 instanceof MetaDataRepositoryException) {
                        response.setStatus(400);
                    }
                    HDApiServlet.logger.error(ex2.getMessage(), (Throwable)ex2);
                    response.resetBuffer();
                    response.getWriter().write(ex2.getMessage());
                }
            }
            catch (ServletException ex3) {}
            catch (NullPointerException ex4) {}
        }
        catch (ExpiredSessionException ex5) {}
        catch (ServletException ex6) {}
        catch (NullPointerException ex7) {}
        catch (MetaDataRepositoryException ex8) {}
        catch (Exception e) {
            HDApiServlet.logger.error((Object)e.getMessage(), (Throwable)e);
            response.setStatus(400);
            response.resetBuffer();
            response.getWriter().write(e.getMessage());
        }
    }
    
    static {
        HDApiServlet.logger = Logger.getLogger((Class)HDApiServlet.class);
        HDApiServlet.predefined = new HashSet<String>() {
            {
                this.add("starttime");
                this.add("endtime");
                this.add("start-time");
                this.add("end-time");
                this.add("key");
                this.add("orderby");
                this.add("sortdir");
                this.add("limit");
                this.add("groupby");
                this.add("singlehds");
                this.add("app-uuid");
                this.add("app-name");
                this.add("time-series-fields");
                this.add("stats-fields");
            }
        };
    }
    
    private static class Paths
    {
        public static final String DEFINITION = "/def";
        public static final String SEARCH = "/search";
    }
}
