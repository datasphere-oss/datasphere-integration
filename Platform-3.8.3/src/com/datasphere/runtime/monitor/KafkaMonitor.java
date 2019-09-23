package com.datasphere.runtime.monitor;

import org.apache.log4j.*;
import com.datasphere.runtime.*;
import com.yammer.metrics.reporting.*;
import java.net.*;
import java.io.*;
import javax.management.remote.*;
import javax.management.*;
import java.util.*;

public class KafkaMonitor implements Runnable
{
    private static Logger logger;
    private final Set<Pair<String, Integer>> serversPorts;
    Long prevTimestamp;
    Long prevMsgs;
    Long prevBytes;
    
    public KafkaMonitor() {
        this.serversPorts = new HashSet<Pair<String, Integer>>();
        this.prevTimestamp = null;
        this.prevMsgs = null;
        this.prevBytes = null;
    }
    
    public void add(final String server, final int port) {
        if (KafkaMonitor.logger.isDebugEnabled()) {
            KafkaMonitor.logger.debug((Object)("Adding kafka broker to listener: " + server + ":" + port));
        }
        this.serversPorts.add(new Pair<String, Integer>(server, port));
    }
    
    @Override
    public void run() {
        for (final Pair<String, Integer> serverPort : this.serversPorts) {
            final String server = serverPort.first;
            final Integer port = serverPort.second;
            if (KafkaMonitor.logger.isDebugEnabled()) {
                KafkaMonitor.logger.debug((Object)("Running kafka monitor for broker: " + server + ":" + port));
            }
            JMXConnector jmxc = null;
            try {
                final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + server + ":" + port + "/jmxrmi");
                jmxc = JMXConnectorFactory.connect(url, null);
                final MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                if (KafkaMonitor.logger.isDebugEnabled()) {
                    System.out.println("\nDomains:");
                    final String[] domains = mbsc.getDomains();
                    Arrays.sort(domains);
                    for (final String domain : domains) {
                        System.out.println("\tDomain = " + domain);
                    }
                    System.out.println("\nMBeanServer default domain = " + mbsc.getDefaultDomain());
                    System.out.println("\nMBean count = " + mbsc.getMBeanCount());
                    System.out.println("\nQuery MBeanServer MBeans:");
                    final Set<ObjectName> names = new TreeSet<ObjectName>(mbsc.queryNames(null, null));
                    for (final ObjectName name : names) {
                        System.out.println("\tObjectName = " + name);
                        final MBeanInfo info = mbsc.getMBeanInfo(name);
                        final MBeanAttributeInfo[] attributes;
                        final MBeanAttributeInfo[] attrs = attributes = info.getAttributes();
                        for (final MBeanAttributeInfo attr : attributes) {
                            System.out.println("\t\tAttr: " + attr);
                            final String[] fieldNames2;
                            final String[] fieldNames = fieldNames2 = attr.getDescriptor().getFieldNames();
                            for (final String fieldName : fieldNames2) {
                                final Object fieldValue = attr.getDescriptor().getFieldValue(fieldName);
                                System.out.println("\t\t\tField name=" + fieldName);
                                System.out.println("\t\t\t     value=" + fieldValue);
                            }
                        }
                    }
                }
                final List<MonitorEvent> eventList = new ArrayList<MonitorEvent>();
                final long timestamp = System.currentTimeMillis();
                ObjectName mbeanName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec");
                JmxReporter.MeterMBean mbeanProxy = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class, true);
                final long kafkaMsgs = mbeanProxy.getCount();
                MonitorEvent event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_MSGS, Long.valueOf(kafkaMsgs), Long.valueOf(timestamp));
                eventList.add(event);
                if (this.prevMsgs != null) {
                    final long delta = Math.abs(kafkaMsgs - this.prevMsgs);
                    final Long rate = 1000L * delta / (timestamp - this.prevTimestamp);
                    event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_MSGS_RATE_LONG, rate, Long.valueOf(timestamp));
                    eventList.add(event);
                    final String value = rate + " msgs/sec";
                    event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_MSGS_RATE, value, Long.valueOf(timestamp));
                    eventList.add(event);
                }
                this.prevMsgs = kafkaMsgs;
                mbeanName = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
                mbeanProxy = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class, true);
                final long kafkaBytes = mbeanProxy.getCount();
                event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_BYTES, Long.valueOf(kafkaBytes), Long.valueOf(timestamp));
                eventList.add(event);
                if (this.prevBytes != null) {
                    final long delta2 = Math.abs(kafkaBytes - this.prevBytes);
                    final Long rate2 = 1000L * delta2 / (timestamp - this.prevTimestamp);
                    event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_BYTES_RATE_LONG, rate2, Long.valueOf(timestamp));
                    eventList.add(event);
                    final String value = rate2 + " bytes/sec";
                    event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.KAFKA_BYTES_RATE, value, Long.valueOf(timestamp));
                    eventList.add(event);
                }
                this.prevBytes = kafkaBytes;
                this.prevTimestamp = timestamp;
                event = new MonitorEvent(MonitorModel.KAFKA_SERVER_UUID, MonitorModel.KAFKA_ENTITY_UUID, MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(timestamp), Long.valueOf(timestamp));
                eventList.add(event);
                final MonitorBatchEvent batchEvent = new MonitorBatchEvent(timestamp, eventList);
                MonitorModel.processBatch(batchEvent);
            }
            catch (MalformedURLException e) {
                KafkaMonitor.logger.error((Object)"Cannot monitor Kafka because the connection URL is invalid", (Throwable)e);
            }
            catch (IOException e2) {
                KafkaMonitor.logger.error((Object)"Cannot monitor Kafka because the connection failed", (Throwable)e2);
            }
            catch (MalformedObjectNameException e3) {
                KafkaMonitor.logger.error((Object)"Cannot monitor Kafka because of malformed object name", (Throwable)e3);
            }
            catch (InstanceNotFoundException e4) {
                KafkaMonitor.logger.error((Object)"Cannot monitor Kafka because of the notification listener subscription failed", (Throwable)e4);
            }
            catch (IntrospectionException e5) {
                KafkaMonitor.logger.error((Object)"Cannot monitor Kafka because of an introspection error", (Throwable)e5);
            }
            catch (ReflectionException e6) {
                KafkaMonitor.logger.error((Object)"Cannot monitor Kafka because of a reflection error", (Throwable)e6);
            }
            catch (Exception e7) {
                KafkaMonitor.logger.error((Object)"Cannot monitor Kafka because of an unknown error", (Throwable)e7);
            }
            finally {
                if (jmxc != null) {
                    try {
                        jmxc.close();
                    }
                    catch (IOException e8) {
                        KafkaMonitor.logger.error((Object)"Failed to close JMX Connection", (Throwable)e8);
                    }
                }
            }
        }
    }
    
    public void addKafkaBrokers(final String kafkaBrokers) {
        if (KafkaMonitor.logger.isDebugEnabled()) {
            KafkaMonitor.logger.debug((Object)("Adding kafka brokers to monitor: " + kafkaBrokers));
        }
        final String[] split;
        final String[] brokers = split = kafkaBrokers.split(",");
        for (final String broker : split) {
            final String[] brokerParts = broker.split(":");
            if (brokerParts.length == 2) {
                final int port = tryParseInt(brokerParts[1], -1);
                if (port != -1) {
                    this.add(brokerParts[0], port);
                }
            }
        }
    }
    
    public static int tryParseInt(final String value, final int defaultValue) {
        try {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    static {
        KafkaMonitor.logger = Logger.getLogger((Class)MonitorModel.class);
    }
}
