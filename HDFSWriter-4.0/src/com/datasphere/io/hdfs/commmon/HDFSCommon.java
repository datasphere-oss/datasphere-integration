package com.datasphere.io.hdfs.commmon;

import com.datasphere.source.lib.prop.*;
import org.apache.log4j.*;
import org.apache.hadoop.conf.*;
import com.datasphere.common.exc.*;
import java.net.*;
import java.nio.file.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import org.apache.hadoop.security.*;
import com.datasphere.common.errors.*;
import com.datasphere.common.errors.Error;

import java.util.*;

public class HDFSCommon
{
    private Property property;
    private String hadoopUrl;
    private Logger logger;
    private boolean kerberosEnabled;
    private String authenticationName;
    private String principal;
    private String keytabPath;
    private boolean appendEnabled;
    
    public HDFSCommon(final Property property) {
        this.logger = Logger.getLogger((Class)HDFSCommon.class);
        this.appendEnabled = false;
        this.property = property;
    }
    
    public FileSystem getHDFSInstance() throws AdapterException {
        this.hadoopUrl = this.property.hadoopUrl;
        final Configuration hdfsConfiguration = new Configuration();
        String hadoopConfigurationPath = this.property.hadoopConfigurationPath;
        if (this.hadoopUrl != null) {
            try {
                final URI uri = new URI(this.hadoopUrl);
                final String scheme = uri.getScheme();
                if (scheme == null) {
                    throw new AdapterException("Invalid HDFS URL " + this.hadoopUrl + " is specified");
                }
                if (!scheme.equalsIgnoreCase("hdfs") && !scheme.equalsIgnoreCase("maprfs") && !scheme.equalsIgnoreCase("wasb")) {
                    throw new AdapterException("Specified HDFS URL " + this.hadoopUrl + " with scheme " + scheme + " is not supported");
                }
            }
            catch (URISyntaxException e1) {
                throw new AdapterException("Failure in constructing URL", (Throwable)e1);
            }
            hdfsConfiguration.set("fs.defaultFS", this.hadoopUrl);
        }
        else if (hadoopConfigurationPath == null) {
            throw new AdapterException("Hadoop URL property is not set. Either set it or set a valid value for HadoopConfigurationPath property, using which Hadoop URL property would be retrieved from reading core-site.xml.");
        }
        if (hadoopConfigurationPath != null) {
            final String separator = FileSystems.getDefault().getSeparator();
            if (!hadoopConfigurationPath.endsWith(separator)) {
                hadoopConfigurationPath += separator;
            }
            try {
                final FileSystem localFileSystem = FileSystem.getLocal(hdfsConfiguration);
                final Path coreSitePath = new Path(hadoopConfigurationPath + "core-site.xml");
                if (!localFileSystem.exists(coreSitePath)) {
                    this.logger.warn((Object)("Specified hadoop configuration path " + hadoopConfigurationPath + " doesn't have core-site.xml file. It is recommended to have this configuration as part of the client. Please make sure correct hadoop configuration path is specified"));
                }
                else {
                    hdfsConfiguration.addResource(coreSitePath);
                    if (this.hadoopUrl == null) {
                        final String hadoopUrl = hdfsConfiguration.get("fs.defaultFS");
                        if (hadoopUrl == null) {
                            throw new AdapterException("HadoopURL property is not set and core-site.xml file in specified HadoopConfigurationPath" + hadoopConfigurationPath + " doesn't have \"fs.defaultFS\" property set");
                        }
                        this.hadoopUrl = hadoopUrl;
                    }
                }
                final Path hdfsSitePath = new Path(hadoopConfigurationPath + "hdfs-site.xml");
                if (!localFileSystem.exists(hdfsSitePath)) {
                    this.logger.warn((Object)("Specified hadoop configuration path " + hadoopConfigurationPath + " doesn't have hdfs-site.xml file. It is recommended to have this configuration as part of the client. Please make sure correct hadoop configuration path is specified"));
                }
                else {
                    hdfsConfiguration.addResource(hdfsSitePath);
                }
                final String appendValue = hdfsConfiguration.get("dfs.support.append");
                if (appendValue != null && appendValue.equalsIgnoreCase("true")) {
                    this.appendEnabled = true;
                }
            }
            catch (IOException e2) {
                throw new AdapterException("Failure in retreiving LocalFileSystem to load resources from the specified configuration path " + hadoopConfigurationPath, (Throwable)e2);
            }
        }
        hdfsConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
        final String authenticationPolicy = this.property.authenticationPolicy;
        if (authenticationPolicy != null) {
            this.validateAuthenticationPolicy(authenticationPolicy);
        }
        if (this.kerberosEnabled) {
            UserGroupInformation.setConfiguration(hdfsConfiguration);
            try {
                UserGroupInformation.loginUserFromKeytab(this.principal, this.keytabPath);
            }
            catch (IOException e2) {
                final AdapterException se = new AdapterException("Problem authenticating kerberos", (Throwable)e2);
                throw se;
            }
        }
        FileSystem hadoopFileSystem;
        try {
            hadoopFileSystem = FileSystem.get(hdfsConfiguration);
            if (this.logger.isInfoEnabled()) {
                this.logger.info((Object)("HDFS is successfully initialized for " + this.hadoopUrl));
            }
        }
        catch (IOException e2) {
            final AdapterException exp = new AdapterException(Error.INVALID_HDFS_CONFIGURATION, (Throwable)e2);
            throw exp;
        }
        return hadoopFileSystem;
    }
    
    public String getHadoopUrl() {
        return this.hadoopUrl;
    }
    
    private void validateAuthenticationPolicy(final String authenticationPolicy) throws AdapterException {
        final Map<String, Object> authenticationPropertiesMap = this.extractAuthenticationProperties(authenticationPolicy);
        if (!this.authenticationName.equalsIgnoreCase("kerberos")) {
            throw new AdapterException("Specified authentication " + this.authenticationName + " is not supported");
        }
        this.kerberosEnabled = true;
        if(authenticationPropertiesMap.get("principal") != null) {
        		this.principal = authenticationPropertiesMap.get("principal").toString();
        }
        if(authenticationPropertiesMap.get("keytabpath") != null) {
        		this.keytabPath = authenticationPropertiesMap.get("keytabpath").toString();
        }
        if (this.principal == null && this.principal.trim().isEmpty() && this.keytabPath == null && this.keytabPath.trim().isEmpty()) {
            throw new AdapterException("Principal or Keytab path required for kerberos authentication cannot be empty or null");
        }
    }
    
    private Map<String, Object> extractAuthenticationProperties(final String authenticationPolicyName) {
        final Map<String, Object> authenticationPropertiesMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        final String[] extractedValues = authenticationPolicyName.split(",");
        boolean isAuthenticationPolicyNameExtracted = false;
        for (final String value : extractedValues) {
            final String[] properties = value.split(":");
            if (properties.length > 1) {
                authenticationPropertiesMap.put(properties[0], properties[1]);
            }
            else if (!isAuthenticationPolicyNameExtracted) {
                this.authenticationName = properties[0];
                isAuthenticationPolicyNameExtracted = true;
            }
        }
        return authenticationPropertiesMap;
    }
    
    public boolean isAppendEnabled() {
        return this.appendEnabled;
    }
}

