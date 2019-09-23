package com.datasphere.hdstore.elasticsearch;

import org.elasticsearch.common.settings.*;
import java.util.*;
import org.elasticsearch.plugins.*;
import org.elasticsearch.node.*;
import org.elasticsearch.cli.*;

public class PluginConfigurableNode extends Node
{
    protected PluginConfigurableNode(final Settings settings, final Collection<Class<? extends Plugin>> classpathPlugins) {
        super(InternalSettingsPreparer.prepareEnvironment(settings, (Terminal)null), (Collection)classpathPlugins);
    }
}
