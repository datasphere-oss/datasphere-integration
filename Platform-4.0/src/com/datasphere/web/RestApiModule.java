package com.datasphere.web;

import com.google.inject.servlet.*;
import org.eclipse.jetty.servlet.*;
import com.google.inject.*;
import com.datasphere.rest.apiv2.appmgmt.*;
import com.datasphere.rest.apiv2.tungsten.*;
import org.codehaus.jackson.jaxrs.*;
import javax.ws.rs.ext.*;
import com.sun.jersey.guice.spi.container.servlet.*;
import java.util.*;

public class RestApiModule extends ServletModule
{
    protected void configureServlets() {
        this.bind((Class)DefaultServlet.class).in((Class)Singleton.class);
        this.bind((Class)ApplicationsApi.class).in((Class)Singleton.class);
        this.bind((Class)TungstenApi.class).in((Class)Singleton.class);
        this.bind((Class)MessageBodyReader.class).to((Class)JacksonJsonProvider.class);
        this.bind((Class)MessageBodyWriter.class).to((Class)JacksonJsonProvider.class);
        final HashMap<String, String> options = new HashMap<String, String>();
        options.put("com.sun.jersey.api.json.POJOMappingFeature", "true");
        options.put("jersey.config.server.provider.packages", "com.datasphere.rest.apiv2");
        this.serve("/*", new String[0]).with((Class)GuiceContainer.class, (Map)options);
    }
}
