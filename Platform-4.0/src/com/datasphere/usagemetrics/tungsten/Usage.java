package com.datasphere.usagemetrics.tungsten;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.metaRepository.MDRepository;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.usagemetrics.cache.UsageMetrics;
import com.datasphere.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class Usage implements Runnable
{
    public static final String COMMAND = "USAGE";
    public static final String DESCRIPTION = "[Display usage information]\n           'USAGE' displays usage information for the entire installation\n           'USAGE <application>' displays usage information for a single application";
    private static final String UNKNOWN_APPLICATION = "Application '%s' does not exist.";
    private static final String NO_USAGE_DATA = "No usage information available.";
    private static final char SEPARATOR = '.';
    private static final Logger logger;
    public static PrintStream reportOutput;
    private final Context context;
    private final String command;
    private final UsageFormat format;
    private final Map<UUID, UsageDescription> usageSummary;
    private String errorMessage;
    private IMap<UUID, UsageMetrics> usageMetricsMap;
    private String namespace;
    private String application;
    
    private Usage(final Context context, final String command) {
        this.usageSummary = new HashMap<UUID, UsageDescription>(8);
        this.context = context;
        this.command = command;
        this.format = new UsageFormat();
    }
    
    public static void execute(final Context context, final String command) {
        if (context == null || command == null) {
            throw new IllegalArgumentException("Arguments cannot be null");
        }
        Usage.logger.info((Object)("Processing command '" + command + '\''));
        final Runnable usage = new Usage(context, command);
        usage.run();
    }
    
    private static String makeQualifiedName(final String schemaName, final String objectName) {
        return schemaName + '.' + objectName;
    }
    
    private static void reportUsage(final Iterable<UsageDescription> entries) {
        for (final UsageDescription entry : entries) {
            report(entry.toString());
        }
    }
    
    private static void calculateMaximumLengths(final Iterable<UsageDescription> entries) {
        for (final UsageDescription entry : entries) {
            entry.calculateMaxLengths();
        }
    }
    
    private static void report(final String text) {
        if (text != null && !text.isEmpty()) {
            Usage.reportOutput.println(text);
            Usage.reportOutput.flush();
        }
    }
    
    @Override
    public void run() {
        try {
            if (this.isCommandValid()) {
                Collection<MetaInfo.MetaObject> sources = null;
                try {
                    sources = com.datasphere.usagemetrics.api.Usage.getSources(this.namespace, this.application, this.context.getAuthToken());
                }
                catch (MetaDataRepositoryException e) {
                    Usage.logger.error((Object)e.getMessage());
                }
                this.loadUsageMetricsMap();
                this.addUsage(sources);
                this.reportUsage();
            }
        }
        catch (MetaDataRepositoryException e2) {
            Usage.logger.error((Object)e2.getMessage());
        }
        report(this.errorMessage);
    }
    
    private void addUsage(final Collection<MetaInfo.MetaObject> sources) {
        for (final MetaInfo.MetaObject source : sources) {
            final UsageMetrics usageMetrics = (this.usageMetricsMap == null) ? null : ((UsageMetrics)this.usageMetricsMap.get((Object)source.uuid));
            this.addUsage(source, usageMetrics);
        }
    }
    
    private boolean isCommandValid() throws MetaDataRepositoryException {
        if (!this.command.toUpperCase().startsWith("USAGE")) {
            return false;
        }
        String commandTarget = this.command.substring("USAGE".length());
        commandTarget = commandTarget.trim();
        if (commandTarget.endsWith(";")) {
            commandTarget = commandTarget.substring(0, commandTarget.length() - 1);
            commandTarget = commandTarget.trim();
        }
        if (commandTarget.isEmpty() || this.isApplicationValid(commandTarget)) {
            return true;
        }
        this.errorMessage = String.format("Application '%s' does not exist.", commandTarget);
        return false;
    }
    
    private boolean isApplicationValid(final String commandTarget) throws MetaDataRepositoryException {
        final EntityType applicationType = EntityType.APPLICATION;
        final int pos = commandTarget.indexOf(46);
        String ns;
        String appName;
        if (pos >= 0) {
            ns = commandTarget.substring(0, pos);
            appName = commandTarget.substring(pos + 1);
        }
        else {
            final MetaInfo.Namespace currentNamespace = this.context.getCurNamespace();
            if (currentNamespace == null) {
                return false;
            }
            ns = currentNamespace.getName();
            appName = commandTarget;
        }
        final MDRepository metaDataRepository = MetadataRepository.getINSTANCE();
        final MetaInfo.MetaObject applicationObject = metaDataRepository.getMetaObjectByName(applicationType, ns, appName, null, this.context.getAuthToken());
        if (applicationObject != null) {
            this.namespace = applicationObject.nsName;
            this.application = applicationObject.name;
            return true;
        }
        return false;
    }
    
    private void loadUsageMetricsMap() {
        final HazelcastInstance hazelcast = HazelcastSingleton.get();
        this.usageMetricsMap = hazelcast.getMap("UsageMetricsMap");
    }
    
    private void addUsage(final MetaInfo.MetaObject object, final UsageMetrics usageMetrics) {
        final String sourceName = makeQualifiedName(object.nsName, object.name);
        final UsageDescription usageDescription = new UsageDescription(this.format, sourceName, usageMetrics);
        this.usageSummary.put(object.uuid, usageDescription);
    }
    
    private void reportUsage() {
        if (this.usageSummary.isEmpty()) {
            this.errorMessage = "No usage information available.";
            return;
        }
        final List<UsageDescription> entries = new ArrayList<UsageDescription>(this.usageSummary.values());
        Collections.sort(entries, new UsageDescriptionComparator());
        calculateMaximumLengths(entries);
        reportUsage(entries);
    }
    
    static {
        logger = Logger.getLogger((Class)Usage.class);
        Usage.reportOutput = System.out;
    }
    
    private static class UsageDescriptionComparator implements Comparator<UsageDescription>, Serializable
    {
        private static final long serialVersionUID = -8445456464178479437L;
        
        @Override
        public int compare(final UsageDescription o1, final UsageDescription o2) {
            final String name1 = o1.sourceName;
            final String name2 = o2.sourceName;
            return name1.compareTo(name2);
        }
    }
}
