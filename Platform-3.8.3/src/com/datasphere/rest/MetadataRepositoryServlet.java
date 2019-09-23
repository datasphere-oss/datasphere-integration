package com.datasphere.rest;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datasphere.drop.DropMetaObject;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.utility.Utility;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class MetadataRepositoryServlet extends MainServlet
{
    private static Logger logger;
    private static final long serialVersionUID = 1L;
    private static MDRepository repo;
    
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        try {
            int responseStatus = 200;
            String responseData = null;
            final Map<String, String> params = this.getParams(request);
            response.setBufferSize(1048576);
            final String type = params.get("type");
            final String uuid = params.get("uuid");
            final String namespace = params.get("nsName");
            final String name = params.get("name");
            final String ver = params.get("version");
            Integer version = null;
            if (ver != null) {
                version = Integer.parseInt(params.get("version"));
            }
            if (uuid == null) {
                if (type == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify either type or uuid").toString();
                }
                else if (type.equals("metatypes")) {
                    responseData = this.getMetaTypes();
                }
            }
            final AuthToken token = this.getToken(request);
            if (token == null) {
                responseStatus = 401;
                responseData = new JSONObject().put("error", (Object)"Must provide login token with request").toString();
            }
            if (responseData == null && uuid != null) {
                try {
                    final UUID uuidobj = new UUID(uuid);
                    final MetaInfo.MetaObject o = MetadataRepositoryServlet.repo.getMetaObjectByUUID(uuidobj, token);
                    if (o != null) {
                        responseData = o.JSONifyString();
                    }
                    else {
                        responseStatus = 404;
                        responseData = new JSONObject().put("error", (Object)"Object with specified UUID not found").toString();
                    }
                }
                catch (StringIndexOutOfBoundsException e7) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"UUID format incorrect").toString();
                }
                catch (MetaDataRepositoryException e) {
                    responseStatus = 500;
                    responseData = new JSONObject().put("error", (Object)e.getMessage()).toString();
                }
            }
            final EntityType etype = this.getEntityTypeByPath(type);
            if (responseData == null && etype == EntityType.UNKNOWN) {
                responseStatus = 400;
                responseData = new JSONObject().put("error", (Object)"Unknown type").toString();
            }
            if (responseData == null && namespace != null && name != null) {
                try {
                    final MetaInfo.MetaObject o = MetadataRepositoryServlet.repo.getMetaObjectByName(etype, namespace, name, version, token);
                    if (o != null) {
                        responseData = o.JSONifyString();
                    }
                    else {
                        responseStatus = 404;
                        responseData = new JSONObject().put("error", (Object)"Object with specified type, namespace, and name not found").toString();
                    }
                }
                catch (MetaDataRepositoryException e2) {
                    responseStatus = 500;
                    responseData = new JSONObject().put("error", (Object)e2.getMessage()).toString();
                }
            }
            if (responseData == null) {
                final List<MetaInfo.MetaObject> objects = new ArrayList<MetaInfo.MetaObject>();
                try {
                    Set<MetaInfo.MetaObject> objs = null;
                    if (namespace != null) {
                        objs = (Set<MetaInfo.MetaObject>)MetadataRepositoryServlet.repo.getByEntityTypeInNameSpace(etype, namespace, token);
                    }
                    else {
                        objs = (Set<MetaInfo.MetaObject>)MetadataRepositoryServlet.repo.getByEntityType(etype, token);
                    }
                    if (objs != null) {
                        if (etype == EntityType.FLOW || etype == EntityType.APPLICATION) {
                            final List<MetaInfo.Flow> flowObjs = new ArrayList<MetaInfo.Flow>();
                            for (final MetaInfo.MetaObject o2 : objs) {
                                flowObjs.add((MetaInfo.Flow)o2);
                            }
                            Utility.removeDroppedFlow(flowObjs);
                            objects.addAll(flowObjs);
                        }
                        else {
                            objects.addAll(objs);
                        }
                        final JSONArray objectsJ = new JSONArray();
                        for (final MetaInfo.MetaObject o2 : objects) {
                            objectsJ.put((Object)o2.JSONify());
                        }
                        responseData = objectsJ.toString();
                    }
                    else {
                        responseStatus = 404;
                        responseData = new JSONObject().put("error", (Object)"No objects found").toString();
                    }
                }
                catch (MetaDataRepositoryException e3) {
                    responseStatus = 500;
                    responseData = new JSONObject().put("error", (Object)e3.getMessage()).toString();
                }
            }
            if (responseStatus == 200) {
                String hashtext = null;
                try {
                    final MessageDigest m = MessageDigest.getInstance("MD5");
                    m.reset();
                    m.update(responseData.getBytes());
                    final byte[] digest = m.digest();
                    final BigInteger bigInt = new BigInteger(1, digest);
                    for (hashtext = bigInt.toString(16); hashtext.length() < 32; hashtext = "0" + hashtext) {}
                    response.addHeader("ETag", hashtext);
                }
                catch (NoSuchAlgorithmException e4) {
                    e4.printStackTrace();
                }
                final String ETag = request.getHeader("If-None-Match");
                if (hashtext != null && hashtext.equals(ETag)) {
                    responseStatus = 304;
                }
            }
            response.setStatus(responseStatus);
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            final byte[] bytes = responseData.getBytes();
            response.setContentLength(bytes.length);
            try {
                response.getOutputStream().write(bytes);
            }
            catch (Exception e5) {
                final String requestString = type + ":" + namespace + "." + name + "." + ver + ":" + uuid;
                MetadataRepositoryServlet.logger.error((Object)("Problem writing response " + responseData + " for request " + requestString), (Throwable)e5);
                throw new RuntimeException("Problem writing response " + responseData + " for request " + requestString);
            }
        }
        catch (Exception e6) {
            MetadataRepositoryServlet.logger.error((Object)"Problem processing metadata request", (Throwable)e6);
        }
    }
    
    private String getMetaTypes() {
        final List<EntityType> entities = new ArrayList<EntityType>();
        entities.addAll(Arrays.asList(EntityType.values()));
        final JSONArray entitiesJ = new JSONArray();
        for (final EntityType entity : entities) {
            final JSONObject entityJ = new JSONObject();
            try {
                entityJ.put("name", (Object)entity.name());
            }
            catch (JSONException e) {
                e.printStackTrace();
            }
            entitiesJ.put((Object)entityJ);
        }
        return entitiesJ.toString();
    }
    
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        try {
            int responseStatus = 200;
            String responseData = null;
            final Map<String, String> params = this.getParams(request);
            final String type = params.get("type");
            final String uuid = params.get("uuid");
            final String namespace = params.get("nsName");
            final String name = params.get("name");
            if (uuid != null) {
                responseStatus = 400;
                responseData = new JSONObject().put("error", (Object)"Cannot post to individual objects. To update objects, use PUT. To add new object, post to collection").toString();
            }
            if (responseData == null) {
                if (namespace == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify namespace").toString();
                }
                else if (name == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify name").toString();
                }
                else if (type == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify type").toString();
                }
            }
            if (responseData == null) {
                final EntityType etype = this.getEntityTypeByPath(type);
                final AuthToken token = this.getToken(request);
                if (token == null) {
                    response.setStatus(401);
                    return;
                }
                MetaInfo.MetaObject o = null;
                try {
                    o = MetadataRepositoryServlet.repo.getMetaObjectByName(etype, namespace, name, null, token);
                }
                catch (MetaDataRepositoryException e) {
                    e.printStackTrace();
                }
                if (o != null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Object by that namespace and name combination already exists").toString();
                }
                else {
                    try {
                        switch (etype) {
                            case DASHBOARD: {
                                o = this.updateDashboard(namespace, name, request, token);
                                break;
                            }
                            case PAGE: {
                                o = this.updatePage(namespace, name, request, token);
                                break;
                            }
                            case QUERYVISUALIZATION: {
                                o = this.updateQueryVisualization(namespace, name, request, token);
                                break;
                            }
                        }
                    }
                    catch (MetaDataRepositoryException e) {
                        e.printStackTrace();
                    }
                    if (o != null) {
                        responseData = o.JSONifyString();
                    }
                }
            }
            if (responseData == null) {
                responseStatus = 405;
                responseData = new JSONObject().put("error", (Object)"Method Unsupported").toString();
            }
            response.setStatus(responseStatus);
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write(responseData);
        }
        catch (JSONException e2) {
            e2.printStackTrace();
        }
    }
    
    protected void doPut(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        try {
            int responseStatus = 200;
            String responseData = null;
            final Map<String, String> params = this.getParams(request);
            final String type = params.get("type");
            final String uuid = params.get("uuid");
            String namespace = params.get("nsName");
            String name = params.get("name");
            if (uuid == null) {
                if (namespace == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify namespace").toString();
                }
                else if (name == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify name").toString();
                }
                else if (type == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify type").toString();
                }
            }
            if (responseData == null) {
                final AuthToken token = this.getToken(request);
                if (token == null) {
                    response.setStatus(401);
                    return;
                }
                EntityType etype = EntityType.UNKNOWN;
                if (uuid != null) {
                    try {
                        final MetaInfo.MetaObject o = MetadataRepositoryServlet.repo.getMetaObjectByUUID(new UUID(uuid), token);
                        etype = o.getType();
                        name = o.getName();
                        namespace = o.getNsName();
                    }
                    catch (MetaDataRepositoryException e) {
                        e.printStackTrace();
                    }
                }
                else {
                    etype = this.getEntityTypeByPath(type);
                }
                try {
                    MetaInfo.MetaObject o = null;
                    switch (etype) {
                        case DASHBOARD: {
                            o = this.updateDashboard(namespace, name, request, token);
                            break;
                        }
                        case PAGE: {
                            o = this.updatePage(namespace, name, request, token);
                            break;
                        }
                        case QUERYVISUALIZATION: {
                            o = this.updateQueryVisualization(namespace, name, request, token);
                            break;
                        }
                    }
                    if (o != null) {
                        responseData = o.JSONifyString();
                    }
                }
                catch (MetaDataRepositoryException e) {
                    e.printStackTrace();
                }
            }
            if (responseData == null) {
                responseStatus = 405;
                responseData = new JSONObject().put("error", (Object)"Object not found or unsupported").toString();
            }
            response.setStatus(responseStatus);
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write(responseData);
        }
        catch (JSONException e2) {
            e2.printStackTrace();
        }
    }
    
    protected void doDelete(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        try {
            int responseStatus = 200;
            String responseData = null;
            final Map<String, String> params = this.getParams(request);
            final String type = params.get("type");
            final String uuid = params.get("uuid");
            final String namespace = params.get("nsName");
            final String name = params.get("name");
            if (uuid == null) {
                if (namespace == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify namespace").toString();
                }
                else if (name == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify name").toString();
                }
                else if (type == null) {
                    responseStatus = 400;
                    responseData = new JSONObject().put("error", (Object)"Must specify type").toString();
                }
            }
            if (responseData == null) {
                final AuthToken token = this.getToken(request);
                if (token == null) {
                    response.setStatus(401);
                    return;
                }
                EntityType etype = EntityType.UNKNOWN;
                if (uuid != null) {
                    try {
                        final UUID u = new UUID(uuid);
                        final MetaInfo.MetaObject o = MetadataRepositoryServlet.repo.getMetaObjectByUUID(u, token);
                        if (o != null) {
                            etype = o.getType();
                        }
                    }
                    catch (MetaDataRepositoryException e) {
                        e.printStackTrace();
                    }
                }
                else {
                    etype = this.getEntityTypeByPath(type);
                }
                try {
                    switch (etype) {
                        case DASHBOARD: {
                            if (uuid != null) {
                                final UUID u = new UUID(uuid);
                                final MetaInfo.MetaObject o = MetadataRepositoryServlet.repo.getMetaObjectByUUID(u, token);
                                if (o != null) {
                                    DropMetaObject.DropDashboard.drop(null, o, DropMetaObject.DropRule.NONE, token);
                                    responseData = new JSONObject().toString();
                                }
                                else {
                                    responseData = new JSONObject().put("error", (Object)"Not Found").toString();
                                }
                                break;
                            }
                            final MetaInfo.MetaObject o = MetadataRepositoryServlet.repo.getMetaObjectByName(etype, namespace, name, null, token);
                            if (o != null) {
                                DropMetaObject.DropDashboard.drop(null, o, DropMetaObject.DropRule.NONE, token);
                                responseData = new JSONObject().toString();
                                break;
                            }
                            responseData = new JSONObject().put("error", (Object)"Not Found").toString();
                            break;
                        }
                        case PAGE:
                        case QUERYVISUALIZATION: {
                            if (uuid == null) {
                                final MetaInfo.MetaObject o = MetadataRepositoryServlet.repo.getMetaObjectByName(etype, namespace, name, null, token);
                                if (o != null) {
                                    MetadataRepositoryServlet.repo.removeMetaObjectByName(etype, namespace, name, null, token);
                                    responseData = new JSONObject().toString();
                                    break;
                                }
                                responseData = new JSONObject().put("error", (Object)"Not Found").toString();
                                break;
                            }
                            else {
                                final UUID u = new UUID(uuid);
                                final MetaInfo.MetaObject o = MetadataRepositoryServlet.repo.getMetaObjectByUUID(u, token);
                                if (o != null) {
                                    MetadataRepositoryServlet.repo.removeMetaObjectByUUID(u, token);
                                    responseData = new JSONObject().toString();
                                    break;
                                }
                                responseData = new JSONObject().put("error", (Object)"Not Found").toString();
                                break;
                            }
                        }
                    }
                }
                catch (MetaDataRepositoryException e) {
                    e.printStackTrace();
                }
            }
            if (responseData == null) {
                responseStatus = 404;
                responseData = new JSONObject().put("error", (Object)"Object not found or unsupported").toString();
            }
            response.setStatus(responseStatus);
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write(responseData);
        }
        catch (JSONException e2) {
            e2.printStackTrace();
        }
    }
    
    private MetaInfo.Dashboard updateDashboard(final String namespace, final String name, final HttpServletRequest request, final AuthToken token) throws IndexOutOfBoundsException, MetaDataRepositoryException {
        MetaInfo.Dashboard d = (MetaInfo.Dashboard)MetadataRepositoryServlet.repo.getMetaObjectByName(EntityType.DASHBOARD, namespace, name, null, token);
        boolean isNew = false;
        if (d == null) {
            d = this.createDashboard(namespace, name, token);
            isNew = true;
        }
        String title = null;
        String defaultpage = null;
        JSONArray pages = null;
        final String model = request.getParameter("model");
        if (model != null) {
            try {
                final JSONObject obj = new JSONObject(model);
                if (obj.has("title")) {
                    title = obj.getString("title");
                }
                if (obj.has("defaultLandingPage")) {
                    defaultpage = obj.getString("defaultLandingPage");
                }
                if (obj.has("pages")) {
                    pages = obj.getJSONArray("pages");
                }
            }
            catch (JSONException e) {
                e.printStackTrace();
            }
        }
        if (title != null) {
            d.setTitle(title);
        }
        if (defaultpage != null) {
            d.setDefaultLandingPage(defaultpage);
        }
        if (pages != null) {
            try {
                final List<String> allPages = new ArrayList<String>();
                for (int i = 0; i < pages.length(); ++i) {
                    allPages.add(pages.get(i).toString());
                }
                d.setPages(allPages);
            }
            catch (JSONException e) {
                e.printStackTrace();
            }
        }
        if (isNew) {
            MetadataRepositoryServlet.repo.putMetaObject(d, token);
        }
        else {
            MetadataRepositoryServlet.repo.updateMetaObject(d, token);
        }
        return d;
    }
    
    private MetaInfo.Page updatePage(final String namespace, final String name, final HttpServletRequest request, final AuthToken token) throws IndexOutOfBoundsException, MetaDataRepositoryException {
        MetaInfo.Page p = (MetaInfo.Page)MetadataRepositoryServlet.repo.getMetaObjectByName(EntityType.PAGE, namespace, name, null, token);
        boolean isNew = false;
        if (p == null) {
            p = this.createPage(namespace, name, token);
            isNew = true;
        }
        String title = null;
        JSONArray visualizations = null;
        String gridJSON = null;
        final String model = request.getParameter("model");
        if (model != null) {
            try {
                final JSONObject obj = new JSONObject(model);
                if (obj.has("title")) {
                    title = obj.getString("title");
                }
                if (obj.has("queryVisualizations")) {
                    visualizations = obj.getJSONArray("queryVisualizations");
                }
                if (obj.has("gridJSON")) {
                    gridJSON = obj.getString("gridJSON");
                }
            }
            catch (JSONException e) {
                e.printStackTrace();
            }
        }
        if (title != null) {
            p.setTitle(title);
        }
        if (visualizations != null) {
            try {
                final List<String> allVisualizations = new ArrayList<String>();
                for (int i = 0; i < visualizations.length(); ++i) {
                    allVisualizations.add(visualizations.get(i).toString());
                }
                p.setQueryVisualizations(allVisualizations);
            }
            catch (JSONException e) {
                e.printStackTrace();
            }
        }
        if (gridJSON != null) {
            p.setGridJSON(gridJSON);
        }
        if (isNew) {
            MetadataRepositoryServlet.repo.putMetaObject(p, token);
        }
        else {
            MetadataRepositoryServlet.repo.updateMetaObject(p, token);
        }
        return p;
    }
    
    private MetaInfo.QueryVisualization updateQueryVisualization(final String namespace, final String name, final HttpServletRequest request, final AuthToken token) throws IndexOutOfBoundsException, MetaDataRepositoryException {
        MetaInfo.QueryVisualization uic = (MetaInfo.QueryVisualization)MetadataRepositoryServlet.repo.getMetaObjectByName(EntityType.QUERYVISUALIZATION, namespace, name, null, token);
        boolean isNew = false;
        if (uic == null) {
            uic = this.createQueryVisualization(namespace, name, token);
            isNew = true;
        }
        String title = null;
        String query = null;
        String visualizationType = null;
        String config = null;
        final String model = request.getParameter("model");
        if (model != null) {
            try {
                final JSONObject obj = new JSONObject(model);
                if (obj.has("title")) {
                    title = obj.getString("title");
                }
                if (obj.has("query")) {
                    query = obj.getString("query");
                }
                if (obj.has("visualizationType")) {
                    visualizationType = obj.getString("visualizationType");
                }
                if (obj.has("config")) {
                    config = obj.getString("config");
                }
            }
            catch (JSONException e) {
                e.printStackTrace();
            }
        }
        if (title != null) {
            uic.setTitle(title);
        }
        if (query != null) {
            uic.setQuery(query);
        }
        if (visualizationType != null) {
            uic.setVisualizationType(visualizationType);
        }
        if (config != null) {
            uic.setConfig(config);
        }
        if (isNew) {
            MetadataRepositoryServlet.repo.putMetaObject(uic, token);
        }
        else {
            MetadataRepositoryServlet.repo.updateMetaObject(uic, token);
        }
        return uic;
    }
    
    private MetaInfo.Dashboard createDashboard(final String namespace, final String name, final AuthToken token) throws MetaDataRepositoryException {
        final MetaInfo.Dashboard d = new MetaInfo.Dashboard();
        final MetaInfo.Namespace ns = (MetaInfo.Namespace)MetadataRepositoryServlet.repo.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
        d.construct(name, ns, EntityType.DASHBOARD);
        return d;
    }
    
    private MetaInfo.Page createPage(final String namespace, final String name, final AuthToken token) throws MetaDataRepositoryException {
        final MetaInfo.Page p = new MetaInfo.Page();
        final MetaInfo.Namespace ns = (MetaInfo.Namespace)MetadataRepositoryServlet.repo.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
        p.construct(name, ns, EntityType.PAGE);
        return p;
    }
    
    private MetaInfo.QueryVisualization createQueryVisualization(final String namespace, final String name, final AuthToken token) throws MetaDataRepositoryException {
        final MetaInfo.QueryVisualization uic = new MetaInfo.QueryVisualization();
        final MetaInfo.Namespace ns = (MetaInfo.Namespace)MetadataRepositoryServlet.repo.getMetaObjectByName(EntityType.NAMESPACE, "Global", namespace, null, token);
        uic.construct(name, ns, EntityType.QUERYVISUALIZATION);
        return uic;
    }
    
    private Map<String, String> getParams(final HttpServletRequest request) {
        final String path = request.getPathInfo();
        final Map<String, String> params = new HashMap<String, String>();
        String type = null;
        String uuid = null;
        String namespace = null;
        String name = null;
        if (path != null) {
            final String[] parts = path.split("/");
            if (parts.length > 1) {
                type = parts[1];
            }
            if (parts.length > 2) {
                if (parts[2].indexOf(46) > 1) {
                    final String[] fullname = parts[2].split("\\.");
                    namespace = fullname[0];
                    name = fullname[1];
                }
                else {
                    uuid = parts[2];
                }
            }
        }
        if (type == null) {
            type = request.getParameter("type");
        }
        if (uuid == null) {
            uuid = request.getParameter("uuid");
        }
        if (namespace == null) {
            namespace = request.getParameter("nsName");
        }
        if (name == null) {
            name = request.getParameter("name");
        }
        if (uuid == null && namespace == null && name == null) {
            final String model = request.getParameter("model");
            if (model != null) {
                try {
                    final JSONObject obj = new JSONObject(model);
                    if (obj.has("type")) {
                        type = obj.getString("type");
                    }
                    if (obj.has("uuid")) {
                        uuid = obj.getString("uuid");
                    }
                    if (obj.has("nsName")) {
                        namespace = obj.getString("nsName");
                    }
                    if (obj.has("name")) {
                        name = obj.getString("name");
                    }
                }
                catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
        final String version = request.getParameter("version");
        params.put("type", type);
        params.put("uuid", uuid);
        params.put("nsName", namespace);
        params.put("name", name);
        params.put("version", version);
        return params;
    }
    
    private EntityType getEntityTypeByPath(final String stype) {
        EntityType type = EntityType.UNKNOWN;
        switch (stype) {
            case "template":
            case "templates": {
                type = EntityType.PROPERTYTEMPLATE;
                break;
            }
            case "role":
            case "roles": {
                type = EntityType.ROLE;
                break;
            }
            case "server":
            case "servers": {
                type = EntityType.SERVER;
                break;
            }
            case "app":
            case "apps": {
                type = EntityType.APPLICATION;
                break;
            }
            case "namespace":
            case "namespaces": {
                type = EntityType.NAMESPACE;
                break;
            }
            case "flow":
            case "flows": {
                type = EntityType.FLOW;
                break;
            }
            case "source":
            case "sources": {
                type = EntityType.SOURCE;
                break;
            }
            case "cache":
            case "caches": {
                type = EntityType.CACHE;
                break;
            }
            case "window":
            case "windows": {
                type = EntityType.WINDOW;
                break;
            }
            case "stream":
            case "streams": {
                type = EntityType.STREAM;
                break;
            }
            case "cq":
            case "cqs": {
                type = EntityType.CQ;
                break;
            }
            case "target":
            case "targets": {
                type = EntityType.TARGET;
                break;
            }
            case "type":
            case "types": {
                type = EntityType.TYPE;
                break;
            }
            case "hdstore":
            case "hdstores": {
                type = EntityType.HDSTORE;
                break;
            }
            case "dashboard":
            case "dashboards": {
                type = EntityType.DASHBOARD;
                break;
            }
            case "page":
            case "pages": {
                type = EntityType.PAGE;
                break;
            }
            case "queryvisualization":
            case "queryvisualizations": {
                type = EntityType.QUERYVISUALIZATION;
                break;
            }
            case "query":
            case "queries": {
                type = EntityType.QUERY;
                break;
            }
        }
        return type;
    }
    
    static {
        MetadataRepositoryServlet.logger = Logger.getLogger((Class)MetadataRepositoryServlet.class);
        MetadataRepositoryServlet.repo = MetadataRepository.getINSTANCE();
    }
}
