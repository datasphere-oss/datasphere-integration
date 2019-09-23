package com.datasphere.web;

import org.apache.log4j.*;
import org.eclipse.jetty.server.nio.*;
import org.eclipse.jetty.server.ssl.*;
import java.util.concurrent.*;
import org.eclipse.jetty.util.ssl.*;
import com.google.inject.*;
import org.eclipse.jetty.server.*;
import com.google.inject.servlet.*;
import org.eclipse.jetty.server.handler.*;
import javax.servlet.http.*;
import org.eclipse.jetty.servlet.*;
import javax.servlet.*;
import java.net.*;
import java.io.*;
import java.util.*;
import com.datasphere.metaRepository.*;
import com.datasphere.hd.*;

public class WebServer extends Server
{
    private static Logger logger;
    SelectChannelConnector connector;
    SslSelectChannelConnector ssl_connector;
    private static ConcurrentHashMap<RMIWebSocket, Boolean> members;
    private static int startHttpPort;
    private static int startHttpsPort;
    private static int end;
    private int httpPort;
    private int httpsPort;
    String bindingInterface;
    
    public int getHttpPort() {
        return this.httpPort;
    }
    
    public int getHttpsPort() {
        return this.httpsPort;
    }
    
    private String getValueFromProps(final String envVariable, final String systemProperty, final String defaultVal) {
        String value = null;
        final String propVal = System.getProperty(systemProperty);
        if (propVal != null) {
            value = propVal;
        }
        else {
            final String envVal = System.getenv(envVariable);
            if (envVal != null) {
                value = envVal;
            }
            else {
                value = defaultVal;
            }
        }
        return value;
    }
    
    public WebServer(final String bindingInterface) {
        this.connector = null;
        this.ssl_connector = null;
        this.httpPort = -1;
        this.httpsPort = -1;
        this.bindingInterface = bindingInterface;
        final String enableHttpVal = this.getValueFromProps("HD_HTTP_ENABLE", "com.datasphere.config.http.enable", "TRUE");
        final boolean enableHttp = enableHttpVal.equalsIgnoreCase("TRUE");
        final String enableHttpsVal = this.getValueFromProps("HD_HTTPS_ENABLE", "com.datasphere.config.https.enable", "TRUE");
        final boolean enableHttps = enableHttpsVal.equalsIgnoreCase("TRUE");
        this.readPortsFromSysprops();
        if (enableHttp) {
            this.connector = new SelectChannelConnector();
            this.httpPort = this.findFreePort(WebServer.startHttpPort);
            if (this.httpPort == -1) {
                throw new RuntimeException("Could not allocate port for http connection");
            }
            this.connector.setPort(this.httpPort);
        }
        if (enableHttps) {
            this.ssl_connector = new SslSelectChannelConnector();
            this.httpsPort = this.findFreePort(WebServer.startHttpsPort);
            if (this.httpsPort == -1) {
                throw new RuntimeException("Could not allocate port for https connection");
            }
            this.ssl_connector.setPort(this.httpsPort);
            final SslContextFactory ssl_cf = this.ssl_connector.getSslContextFactory();
            final String keyStorePathDefault = this.getClass().getResource("keystore.jks").toExternalForm();
            final String keyStorePasswordDefault = "WA:HTTPSKSP";
            final String keyStoreManagerPasswordDefault = "WA:HTTPSKSP";
            final String keyStorePath = this.getValueFromProps("HD_HTTPS_KEYSTORE_PATH", "com.datasphere.config.https.keystore.path", keyStorePathDefault);
            ssl_cf.setKeyStorePath(keyStorePath);
            final String keyStorePassword = this.getValueFromProps("HD_HTTPS_KEYSTORE_PASSWORD", "com.datasphere.config.https.keystore.password", keyStorePasswordDefault);
            ssl_cf.setKeyStorePassword(keyStorePassword);
            final String keyStoreManagerPassword = this.getValueFromProps("HD_HTTPS_KEYSTORE_MANAGERPASSWORD", "com.datasphere.config.https.keystore.managerpassword", keyStoreManagerPasswordDefault);
            ssl_cf.setKeyManagerPassword(keyStoreManagerPassword);
        }
        if (WebServer.logger.isInfoEnabled()) {
            WebServer.logger.info((Object)("HTTP Server will start on : " + this.httpPort));
        }
        if (WebServer.logger.isInfoEnabled()) {
            WebServer.logger.info((Object)("HTTPS Server will start on : " + this.httpsPort));
        }
        if (enableHttp && enableHttps) {
            this.setConnectors(new Connector[] { this.connector, this.ssl_connector });
        }
        else if (enableHttp) {
            this.setConnectors(new Connector[] { this.connector });
        }
        else if (enableHttps) {
            this.setConnectors(new Connector[] { this.ssl_connector });
        }
        final ContextHandlerCollection contexts = new ContextHandlerCollection();
        this.setHandler((Handler)contexts);
    }
    
    private void readPortsFromSysprops() {
        final String httpPort = System.getProperty("com.datasphere.config.httpPort");
        if (httpPort != null && !httpPort.isEmpty()) {
            try {
                WebServer.startHttpPort = Integer.parseInt(httpPort);
            }
            catch (NumberFormatException ex) {}
        }
        final String sslPort = System.getProperty("com.datasphere.config.httpsPort");
        if (sslPort != null && !sslPort.isEmpty()) {
            try {
                WebServer.startHttpsPort = Integer.parseInt(sslPort);
            }
            catch (NumberFormatException ex2) {}
        }
        if (WebServer.logger.isDebugEnabled()) {
            WebServer.logger.debug((Object)("Using :" + WebServer.startHttpPort + " as HTTP port."));
        }
        if (WebServer.logger.isDebugEnabled()) {
            WebServer.logger.debug((Object)("Using :" + WebServer.startHttpsPort + " as HTTPS port."));
        }
    }
    
    public void shutdown() throws Exception {
        this.stop();
    }
    
    public void setRestServletContextHandler() {
        Guice.createInjector(Stage.PRODUCTION, new Module[] { new RestApiModule() });
        final ServletContextHandler context = new ServletContextHandler((HandlerContainer)this, "/api/v2", 1);
        context.addFilter((Class)GuiceFilter.class, "/*", (EnumSet)EnumSet.of(DispatcherType.REQUEST, DispatcherType.ASYNC));
        context.addServlet((Class)DefaultServlet.class, "/*");
    }
    
    public void addContextHandler(final String contextPath, final Handler handler) {
        final ContextHandler context = new ContextHandler();
        context.setContextPath(contextPath);
        context.setResourceBase("./" + contextPath);
        context.setHandler(handler);
        this.addContextForHandler(contextPath, context);
    }
    
    private void addContextForHandler(final String contextPath, final ContextHandler context) {
        final Handler[] handlers = this.getHandlers();
        final int len = (handlers == null) ? 0 : handlers.length;
        final Handler[] newHandlers = new Handler[len + 1];
        if (len > 0) {
            System.arraycopy(handlers, 0, newHandlers, 0, handlers.length);
        }
        newHandlers[len] = (Handler)context;
        final ContextHandlerCollection newContexts = new ContextHandlerCollection();
        newContexts.setHandlers(newHandlers);
        this.setHandler((Handler)newContexts);
    }
    
    public void addResourceHandler(final String contextPath, final String resourceBase, final boolean listDirectories, final String[] welcomeFiles) {
        final ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(listDirectories);
        resourceHandler.setWelcomeFiles(welcomeFiles);
        resourceHandler.setResourceBase(resourceBase);
        final GzipHandler gzipHandlerRES = new GzipHandler();
        gzipHandlerRES.setMimeTypes("text/html,text/plain,text/xml,text/css,application/javascript,text/javascript,text/x-javascript,application/x-javascript");
        gzipHandlerRES.setHandler((Handler)resourceHandler);
        this.addContextHandler(contextPath, (Handler)gzipHandlerRES);
    }
    
    public void addServletContextHandler(final String path, final HttpServlet servlet) {
        final ServletContextHandler ctx = new ServletContextHandler(1);
        ctx.setContextPath("/");
        ctx.addServlet(new ServletHolder((Servlet)servlet), path);
        this.addContextForHandler(path, (ContextHandler)ctx);
    }
    
    public boolean available(final int start, final int port) {
        if (port < start || port > WebServer.end) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }
        if (this.httpPort != -1 && port == this.httpPort) {
            return false;
        }
        if (this.httpsPort != -1 && port == this.httpsPort) {
            return false;
        }
        Socket ss = null;
        Enumeration<NetworkInterface> nets = null;
        try {
            nets = NetworkInterface.getNetworkInterfaces();
        }
        catch (SocketException e) {
            WebServer.logger.error((Object)"Could not get any network interfaces: ", (Throwable)e);
            throw new RuntimeException("No network interfaces available for web server connection");
        }
        for (final NetworkInterface netint : Collections.list(nets)) {
            final Enumeration<InetAddress> addrs = netint.getInetAddresses();
            for (final InetAddress addr : Collections.list(addrs)) {
                if (!"/127.0.0.1".equals(addr.toString())) {
                    if (!("/" + this.bindingInterface).equals(addr.toString())) {
                        continue;
                    }
                }
                try {
                    ss = new Socket(addr, port);
                    return false;
                }
                catch (IOException ex) {}
                finally {
                    if (ss != null) {
                        try {
                            ss.close();
                        }
                        catch (IOException ex2) {}
                    }
                }
            }
        }
        return true;
    }
    
    public int findFreePort(final int start) {
        for (int port = start; port < WebServer.end; ++port) {
            if (this.available(start, port)) {
                return port;
            }
        }
        return -1;
    }
    
    public static void addMember(final RMIWebSocket ws) {
        WebServer.members.putIfAbsent(ws, true);
    }
    
    public static void removeMember(final RMIWebSocket ws) {
        WebServer.members.remove(ws);
    }
    
    public static ConcurrentHashMap<RMIWebSocket, Boolean> getMembers() {
        return WebServer.members;
    }
    
    public static void main() {
        final WebServer server = new WebServer("127.0.0.1");
        try {
            RMIWebSocket.register(MDCache.getInstance());
            RMIWebSocket.register(HDApi.get());
            server.addContextHandler("/rmiws", (Handler)new RMIWebSocket.Handler());
            server.addResourceHandler("/web", "./web", true, new String[] { "index.html" });
            server.addResourceHandler("/flow", "../FlowDesigner", true, new String[] { "index.html" });
            server.addResourceHandler("/dash", "../dashboard", true, new String[] { "index.html" });
            server.start();
        }
        catch (Exception e) {
            WebServer.logger.error((Object)e);
        }
    }
    
    static {
        WebServer.logger = Logger.getLogger((Class)WebServer.class);
        WebServer.members = new ConcurrentHashMap<RMIWebSocket, Boolean>();
        WebServer.startHttpPort = 9080;
        WebServer.startHttpsPort = 9081;
        WebServer.end = 65535;
    }
    
    public static class TempConversionService
    {
        public double onTempChangeF(final double val) {
            return this.onTempChange(true, val);
        }
        
        public String sayHi(final String toWho) {
            return "Hi " + toWho;
        }
        
        public double onTempChangeC(final double val) {
            return this.onTempChange(false, val);
        }
        
        private double onTempChange(final boolean isFarenheit, final double val) {
            return isFarenheit ? ((val - 32.0) * 5.0 / 9.0) : (val * 9.0 / 5.0 + 32.0);
        }
    }
}
