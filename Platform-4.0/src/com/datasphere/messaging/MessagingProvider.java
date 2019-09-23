package com.datasphere.messaging;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.uuid.UUID;

public class MessagingProvider
{
    public static String ZMQ_SYSTEM;
    public static String KAFKA_SYSTEM;
    private static Logger logger;
    private static Map<String, MessagingSystem> ourSystems;
    
    public static MessagingSystem getMessagingSystem(final String fqcn) {
        MessagingSystem ourSystem = MessagingProvider.ourSystems.get(fqcn);
        if (ourSystem == null) {
            try {
                final Class clazz = ClassLoader.getSystemClassLoader().loadClass(fqcn);
                try {
                    String serverIpOrName = HazelcastSingleton.getBindingInterface();
                    final String serverDomainName = System.getProperty("com.datasphere.config.serverDomainName");
                    if (serverDomainName != null && !serverDomainName.isEmpty()) {
                        serverIpOrName = serverDomainName;
                    }
                    final Constructor declaredConstructor = clazz.getDeclaredConstructor(UUID.class, String.class);
                    final UUID nodeId = HazelcastSingleton.getNodeId();
                    ourSystem = (MessagingSystem)declaredConstructor.newInstance(nodeId, serverIpOrName);
                    MessagingProvider.ourSystems.put(fqcn, ourSystem);
                }
                catch (InstantiationException e) {
                    MessagingProvider.logger.error((Object)e.getMessage());
                }
                catch (IllegalAccessException e2) {
                    MessagingProvider.logger.error((Object)e2.getMessage());
                }
                catch (NoSuchMethodException e3) {
                    MessagingProvider.logger.error((Object)e3.getMessage());
                }
                catch (InvocationTargetException e4) {
                    MessagingProvider.logger.error((Object)e4.getMessage());
                }
                catch (ExceptionInInitializerError e5) {
                    MessagingProvider.logger.error((Object)e5.getMessage());
                }
                catch (NoClassDefFoundError e6) {
                    MessagingProvider.logger.error((Object)e6.getMessage());
                }
            }
            catch (ClassNotFoundException e7) {
                MessagingProvider.logger.error((Object)e7.getMessage());
            }
        }
        return ourSystem;
    }
    
    public static void shutdownAll() {
        for (final MessagingSystem ms : MessagingProvider.ourSystems.values()) {
            ms.shutdown();
        }
    }
    
    static {
        MessagingProvider.ZMQ_SYSTEM = "com.datasphere.jmqmessaging.ZMQSystem";
        MessagingProvider.KAFKA_SYSTEM = "com.datasphere.kafkamessaging.KafkaSystem";
        MessagingProvider.logger = Logger.getLogger((Class)MessagingProvider.class);
        MessagingProvider.ourSystems = new HashMap<String, MessagingSystem>();
    }
}
