package com.datasphere.metaRepository;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.datasphere.classloading.HDLoader;
import com.datasphere.discovery.UdpDiscoveryClient;
import com.datasphere.event.SimpleEvent;
import com.hazelcast.azure.AzureProperties;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.nio.serialization.Serializer;
import com.datasphere.runtime.ConsoleReader;
import com.datasphere.runtime.NodeStartUp;
import com.datasphere.runtime.Server;
import com.datasphere.security.HSecurityManager;
import com.datasphere.ser.CustomHazelcastKryoSerializer;
import com.datasphere.uuid.AuthToken;
import com.datasphere.uuid.UUID;

public class HazelcastSingleton
{
    private static Logger logger;
    private static volatile HazelcastInstance instance;
    private static final String default_cluster_name = "TestCluster";
    private static Config config;
    private static volatile UUID nodeId;
    private static final int multicastPort = 54327;
    private static String InterfaceToBind;
    public static boolean passedInterfacesDidNotMatchAvailableInterfaces;
    private static String interfaceValuePassedFromSytemProperties;
    public static int isFirstMember;
    public static Map<UUID, Endpoint> activeClusterMembers;
    private static String macID;
    public static boolean noClusterJoin;
    private static HazelcastMemberAddressProvider hzmemberAddressProvider;
    
    public static void main(final String[] args) {
        get();
    }
    
    public static boolean isFirstMember() {
        if (HazelcastSingleton.isFirstMember == -1) {
            final Cluster c = HazelcastSingleton.instance.getCluster();
            final Set<Member> memberSet = (Set<Member>)c.getMembers();
            final Iterator<Member> iterator = memberSet.iterator();
            if (iterator.hasNext()) {
                final Member member = iterator.next();
                if (c.getLocalMember().equals(member)) {
                    HazelcastSingleton.isFirstMember = 1;
                }
                else {
                    HazelcastSingleton.isFirstMember = 0;
                }
            }
        }
        return HazelcastSingleton.isFirstMember == 1;
    }
    
    public static List<String> getLoopBackIP() {
        final List<String> ip = new ArrayList<String>();
        try {
            if (HazelcastSingleton.logger.isInfoEnabled()) {
                HazelcastSingleton.logger.info((Object)InetAddress.getLocalHost());
            }
            for (final NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (iface.isLoopback() && iface.isUp()) {
                    final List<InterfaceAddress> list = iface.getInterfaceAddresses();
                    for (final InterfaceAddress iadd : list) {
                        if (iadd.getBroadcast() != null || iadd.getAddress().equals(InetAddress.getLocalHost())) {
                            ip.add(iadd.getAddress().getHostAddress());
                            if (!HazelcastSingleton.logger.isInfoEnabled()) {
                                continue;
                            }
                            HazelcastSingleton.logger.info((Object)("Added " + ip + " to list of usable interfaces"));
                        }
                        else {
                            HazelcastSingleton.logger.warn((Object)("Found " + iadd.getAddress().getHostAddress() + " , but does not have broadcast capabilities."));
                        }
                    }
                }
            }
        }
        catch (SocketException | UnknownHostException ex2) {
            HazelcastSingleton.logger.error((Object)ex2.getMessage());
        }
        return ip;
    }
    
    public static List<String> getInterface() {
        final List<String> ip = new ArrayList<String>();
        try {
            for (final NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!iface.isLoopback() && iface.isUp()) {
                    final List<InterfaceAddress> list = iface.getInterfaceAddresses();
                    for (final InterfaceAddress iadd : list) {
                        if (iadd.getBroadcast() != null) {
                            ip.add(iadd.getAddress().getHostAddress());
                            if (!HazelcastSingleton.logger.isInfoEnabled()) {
                                continue;
                            }
                            HazelcastSingleton.logger.info((Object)("Added " + ip + " to list of usable interfaces"));
                        }
                    }
                }
            }
        }
        catch (SocketException e) {
            HazelcastSingleton.logger.error((Object)e.getMessage());
        }
        if (ip.isEmpty()) {
            ip.addAll(getLoopBackIP());
        }
        return ip;
    }
    
    public static UUID getNodeId() {
        if (HazelcastSingleton.nodeId == null) {
            synchronized (HazelcastSingleton.class) {
                if (HazelcastSingleton.nodeId == null) {
                    final String id = get().getLocalEndpoint().getUuid();
                    HazelcastSingleton.nodeId = new UUID(id);
                }
            }
        }
        return HazelcastSingleton.nodeId;
    }
    
    private static String generateRandomMacID() {
        int parts = 6;
        final String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        final int alphabetLength = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".length();
        String randomMacID = new String();
        final Random random = new Random();
        while (parts-- > 0) {
            final Character firstPosition = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".charAt(random.nextInt(alphabetLength));
            final Character secondPosition = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".charAt(random.nextInt(alphabetLength));
            randomMacID = randomMacID + firstPosition.toString() + secondPosition.toString() + "-";
        }
        return randomMacID.substring(0, randomMacID.length() - 1);
    }
    
    public static void setMacID() {
        try {
            final InetAddress address = InetAddress.getByName(HazelcastSingleton.InterfaceToBind);
            final NetworkInterface networkInterface = NetworkInterface.getByInetAddress(address);
            if (networkInterface != null) {
                final byte[] hardwareAddress = networkInterface.getHardwareAddress();
                if (hardwareAddress != null) {
                    final StringBuilder builder = new StringBuilder();
                    for (int ii = 0; ii < hardwareAddress.length; ++ii) {
                        builder.append(String.format("%02X%s", hardwareAddress[ii], (ii < hardwareAddress.length - 1) ? "-" : ""));
                    }
                    HazelcastSingleton.macID = builder.toString();
                }
                else {
                    HazelcastSingleton.macID = generateRandomMacID();
                    if (HazelcastSingleton.logger.isDebugEnabled()) {
                        HazelcastSingleton.logger.debug((Object)("Using a generated MAC ID : " + HazelcastSingleton.macID));
                    }
                }
            }
            else {
                HazelcastSingleton.logger.warn((Object)("The network interface " + HazelcastSingleton.InterfaceToBind + " is currently not available!"));
                System.exit(-1);
            }
        }
        catch (UnknownHostException e) {
            HazelcastSingleton.logger.error((Object)e.getMessage());
        }
        catch (SocketException e2) {
            HazelcastSingleton.logger.error((Object)e2.getMessage());
        }
    }
    
    public static String getMacID() {
        if (HazelcastSingleton.macID == null) {
            get();
        }
        return HazelcastSingleton.macID;
    }
    
    public static String chooseInterface(final List<String> ListOfAvailableInterfaces) {
        for (int i = 0; i < ListOfAvailableInterfaces.size(); ++i) {
            System.out.println(i + 1 + ". " + ListOfAvailableInterfaces.get(i));
        }
        if (ListOfAvailableInterfaces.size() == 0) {
            printf("No available interfaces found to connect, exiting now..");
            System.exit(0);
        }
        if (ListOfAvailableInterfaces.size() == 1) {
            printf("Since there is only 1 interface selecting it by default \n");
            HazelcastSingleton.InterfaceToBind = ListOfAvailableInterfaces.get(0);
        }
        else {
            String s = null;
            try {
                printf("Please enter an option number to choose the corresponding interface : \n");
                while (true) {
                    s = ConsoleReader.readLineRespond("");
                    try {
                        final int option = Integer.parseInt(s);
                    }
                    catch (NumberFormatException e2) {
                        System.err.println("Not a valid input. Enter a number between 1 and " + ListOfAvailableInterfaces.size());
                        continue;
                    }
                    break;
                }
                HazelcastSingleton.InterfaceToBind = ListOfAvailableInterfaces.get(Integer.parseInt(s) - 1);
            }
            catch (IOException e) {
                if (HazelcastSingleton.logger.isInfoEnabled()) {
                    HazelcastSingleton.logger.info((Object)("IO error trying to get an option number" + e.getMessage()));
                }
            }
        }
        return HazelcastSingleton.InterfaceToBind;
    }
    
    public static void initializeBackingStore() {
        if (Server.persistenceIsEnabled()) {
            final MapStoreConfig UUIDToURLStoreCfg = new MapStoreConfig();
            UUIDToURLStoreCfg.setClassName("com.datasphere.metaRepository.UUIDUrlMetaStore").setEnabled(false);
            UUIDToURLStoreCfg.setWriteDelaySeconds(0);
            final MapConfig UUIDToURLMapCfg = getMapConfig("#uuidToUrl");
            UUIDToURLMapCfg.setMapStoreConfig(UUIDToURLStoreCfg);
            HazelcastSingleton.config.addMapConfig(UUIDToURLMapCfg);
            final MapStoreConfig URLToObjectStoreCfg = new MapStoreConfig();
            URLToObjectStoreCfg.setClassName("com.datasphere.metaRepository.UrlObjMetaStore").setEnabled(false);
            URLToObjectStoreCfg.setWriteDelaySeconds(0);
            final MapConfig URLToObjectMapCfg = getMapConfig("#urlToMetaObject");
            URLToObjectMapCfg.setMapStoreConfig(URLToObjectStoreCfg);
            HazelcastSingleton.config.addMapConfig(URLToObjectMapCfg);
        }
    }
    
    public static String getBindingInterface() {
        return HazelcastSingleton.InterfaceToBind;
    }
    
    private static void setInstance(final HazelcastInstance newInstance, final HDLoader loader) {
        synchronized (HazelcastSingleton.class) {
            HazelcastSingleton.instance = newInstance;
            HazelcastSingleton.activeClusterMembers = createActiveMembersMap(newInstance.getCluster());
            loader.init();
        }
    }
    
    public static HazelcastInstance get(final String clusterName, final String passedInterfaces, final HDClusterMemberType memType) throws RuntimeException {
        if (HazelcastSingleton.instance == null) {
            synchronized (HazelcastSingleton.class) {
                if (HazelcastSingleton.instance == null) {
                    return init(clusterName, passedInterfaces, memType);
                }
            }
        }
        return HazelcastSingleton.instance;
    }
    
    public static HazelcastInstance get(final String clusterName, final String passedInterfaces) throws RuntimeException {
        return get(clusterName, passedInterfaces, HDClusterMemberType.SERVERNODE);
    }
    
    public static HazelcastInstance get(final String clusterName, final HDClusterMemberType memType) throws RuntimeException {
        return get(clusterName, null, memType);
    }
    
    public static HazelcastInstance get(final String clusterName) throws RuntimeException {
        return get(clusterName, null, HDClusterMemberType.SERVERNODE);
    }
    
    public static HazelcastInstance get() throws RuntimeException {
        return get(null);
    }
    
    private static HazelcastInstance init(final String clusterName, final String passedInterfaces, final HDClusterMemberType memType) throws RuntimeException {
        if (HazelcastSingleton.logger.isInfoEnabled()) {
            HazelcastSingleton.logger.info((Object)"re-creating new hazelcast instance");
        }
        final Config cfg = createServerConfig(clusterName, passedInterfaces);
        if (HazelcastSingleton.noClusterJoin) {
            final JoinConfig jn = cfg.getNetworkConfig().getJoin();
            jn.getMulticastConfig().setEnabled(false);
            jn.getAwsConfig().setEnabled(false);
            jn.getTcpIpConfig().setEnabled(false);
        }
        final HDLoader loader = HDLoader.get(false);
        cfg.setClassLoader((ClassLoader)loader);
        cfg.setProperty("hazelcast.shutdownhook.enabled", "false");
        cfg.setProperty("hazelcast.wait.seconds.before.join", "1");
        initializeBackingStore();
        Label_0382: {
            if (memType != HDClusterMemberType.SERVERNODE) {
                try {
                    printf("Connecting to cluster " + clusterName + ".");
                    final ClientConfig clientConfig = createClientConfig(clusterName);
                    HazelcastSingleton.instance = HazelcastClient.newHazelcastClient(clientConfig);
                    break Label_0382;
                }
                catch (Exception e) {
                    printf("..Failed\n");
                    throw new RuntimeException("Failed to initialize a client connection to cluster " + clusterName);
                }
            }
            cfg.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass((Class)UUID.class).setImplementation((Serializer)new CustomHazelcastKryoSerializer(UUID.class, 9900)));
            cfg.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass((Class)AuthToken.class).setImplementation((Serializer)new CustomHazelcastKryoSerializer(AuthToken.class, 9901)));
            cfg.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass((Class)SimpleEvent.class).setImplementation((Serializer)new CustomHazelcastKryoSerializer(SimpleEvent.class, 9902)));
            HazelcastSingleton.instance = Hazelcast.newHazelcastInstance(cfg);
            HazelcastSingleton.hzmemberAddressProvider = new SimpleHZMemberAddressProvider(HazelcastSingleton.instance.getConfig().getNetworkConfig().getPublicAddress(), HazelcastSingleton.instance.getConfig().getNetworkConfig().getInterfaces().getInterfaces().iterator().next(), HazelcastSingleton.instance.getConfig().getNetworkConfig().getPort());
        }
        HazelcastSingleton.activeClusterMembers = createActiveMembersMap(HazelcastSingleton.instance.getCluster());
        loader.init();
        return HazelcastSingleton.instance;
    }
    
    public static HazelcastMemberAddressProvider getHzmemberAddressProvider() {
        return HazelcastSingleton.hzmemberAddressProvider;
    }
    
    public static HazelcastInstance initIfPopulated(final String clusterName, final String passedInterfaces) {
        final HDLoader loader = HDLoader.get(false);
        final HazelcastInstance instanceCandidate = createHazelcastInstance(clusterName, passedInterfaces, loader);
        final Set<Member> members = (Set<Member>)instanceCandidate.getCluster().getMembers();
        for (final Member member : members) {
            if (!member.localMember()) {
                setInstance(instanceCandidate, loader);
                return instanceCandidate;
            }
        }
        return null;
    }
    
    private static HazelcastInstance createHazelcastInstance(final String clusterName, final String passedInterfaces, final HDLoader loader) {
        if (HazelcastSingleton.logger.isInfoEnabled()) {
            HazelcastSingleton.logger.info((Object)"re-creating new hazelcast instance");
        }
        final Config cfg = createServerConfig(clusterName, passedInterfaces);
        if (HazelcastSingleton.noClusterJoin) {
            final JoinConfig jn = cfg.getNetworkConfig().getJoin();
            jn.getMulticastConfig().setEnabled(false);
            jn.getAwsConfig().setEnabled(false);
            jn.getTcpIpConfig().setEnabled(false);
        }
        cfg.setClassLoader((ClassLoader)loader);
        cfg.setProperty("hazelcast.shutdownhook.enabled", "false");
        cfg.setProperty("hazelcast.wait.seconds.before.join", "1");
        initializeBackingStore();
        return Hazelcast.newHazelcastInstance(cfg);
    }
    
    public static boolean isClientMember() {
        return HazelcastSingleton.instance != null && HazelcastSingleton.instance instanceof HazelcastClientProxy;
    }
    
    public static String getClusterName() {
        if (HazelcastSingleton.instance == null) {
            throw new IllegalStateException("getClusterName can be called only after initialization");
        }
        if (HazelcastSingleton.instance instanceof HazelcastClientProxy) {
            return ((HazelcastClientProxy)HazelcastSingleton.instance).getClientConfig().getGroupConfig().getName();
        }
        return HazelcastSingleton.instance.getConfig().getGroupConfig().getName();
    }
    
    private static Map<UUID, Endpoint> createActiveMembersMap(final Cluster c) {
        final ConcurrentHashMap<UUID, Endpoint> map = new ConcurrentHashMap<UUID, Endpoint>();
        for (final Member m : c.getMembers()) {
            map.put(new UUID(m.getUuid()), (Endpoint)m);
        }
        c.addMembershipListener((MembershipListener)new MembershipListener() {
            public void memberAdded(final MembershipEvent membershipEvent) {
                final Member m = membershipEvent.getMember();
                map.put(new UUID(m.getUuid()), m);
            }
            
            public void memberRemoved(final MembershipEvent membershipEvent) {
                final Member m = membershipEvent.getMember();
                map.remove(new UUID(m.getUuid()));
            }
            
            public void memberAttributeChanged(final MemberAttributeEvent arg0) {
            }
        });
        if (!isClientMember()) {
            HazelcastSingleton.instance.getClientService().addClientListener((ClientListener)new ClientListener() {
                public void clientDisconnected(final Client client) {
                    HazelcastSingleton.activeClusterMembers.remove(new UUID(client.getUuid()));
                }
                
                public void clientConnected(final Client client) {
                    HazelcastSingleton.activeClusterMembers.put(new UUID(client.getUuid()), (Endpoint)client);
                }
            });
        }
        return map;
    }
    
    public static Map<UUID, Endpoint> getActiveMembersMap() {
        return HazelcastSingleton.activeClusterMembers;
    }
    
    public static void setDBDetailsForMetaDataRepository(final String DBLocation, final String DBName, final String DBUname, final String DBPassword) {
        MetaDataDBOps.setDBDetails(DBLocation, DBName, DBUname, DBPassword);
    }
    
    private static byte[] intToByteArray(final int value) {
        return new byte[] { (byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value };
    }
    
    private static Config createServerConfig(String clusterName, final String passedInterfaces) throws RuntimeException {
        if (clusterName == null) {
            final String cName = System.getProperty("com.datasphere.config.clusterName");
            if (cName == null) {
                clusterName = "TestCluster";
            }
            else {
                clusterName = cName;
            }
        }
        HazelcastSingleton.config = new Config();
        final String partitionGroupType = System.getProperty("com.datasphere.config.hazelcast.partitiongroup");
        if (partitionGroupType != null && partitionGroupType.equalsIgnoreCase("HOST_AWARE")) {
            HazelcastSingleton.config.getPartitionGroupConfig().setEnabled(true).setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
        }
        final NetworkConfig network = HazelcastSingleton.config.getNetworkConfig();
        final JoinConfig join = network.getJoin();
        final String isEc2Cluster = System.getProperty("com.datasphere.config.enable-ec2Clustering");
        final Boolean isEc2 = new Boolean(isEc2Cluster != null && isEc2Cluster.equalsIgnoreCase("TRUE"));
        final String isTcpIpCluster = System.getProperty("com.datasphere.config.enable-tcpipClustering");
        final Boolean isTcpIp = new Boolean(isTcpIpCluster != null && isTcpIpCluster.equalsIgnoreCase("TRUE"));
        final String isAzureCluster = System.getProperty("com.datasphere.config.enable-azureClustering", "False");
        final Boolean isAzure = new Boolean(isAzureCluster != null && isAzureCluster.equalsIgnoreCase("TRUE"));
        if (isEc2) {
            printf("Using Ec2 clustering to discover the cluster members\n");
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(false);
            join.getAwsConfig().setEnabled(true);
            if (System.getProperty("com.datasphere.config.enable-ec2AccessKey") != null && System.getProperty("com.datasphere.config.enable-ec2SecretKey") != null) {
                join.getAwsConfig().setAccessKey(System.getProperty("com.datasphere.config.enable-ec2AccessKey", System.getenv("DSS_EC2_ACCESS_KEY")));
                join.getAwsConfig().setSecretKey(System.getProperty("com.datasphere.config.enable-ec2SecretKey", System.getenv("DSS_EC2_SECRET_KEY")));
            }
            if (System.getProperty("com.datasphere.config.ec2IamRole") != null) {
                join.getAwsConfig().setIamRole(System.getProperty("com.datasphere.config.ec2IamRole"));
            }
            final String region = System.getProperty("com.datasphere.config.enable-ec2Region", "us-east-1");
            join.getAwsConfig().setHostHeader("ec2." + region + ".amazonaws.com");
            join.getAwsConfig().setRegion(region);
            join.getAwsConfig().setSecurityGroupName(System.getProperty("com.datasphere.config.enable-ec2SecurityGroup"));
        }
        else if (isAzure) {
            printf("Using Azure clustering to discover the cluster members\n");
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(false);
            join.getAwsConfig().setEnabled(false);
            final Map<String, Comparable> properties = new HashMap<String, Comparable>();
            properties.put(AzureProperties.CLIENT_ID.key(), System.getProperty("com.datasphere.config.azure.clientId"));
            properties.put(AzureProperties.CLIENT_SECRET.key(), System.getProperty("com.datasphere.config.azure.clientSecret"));
            properties.put(AzureProperties.TENANT_ID.key(), System.getProperty("com.datasphere.config.azure.tenantId"));
            properties.put(AzureProperties.SUBSCRIPTION_ID.key(), System.getProperty("com.datasphere.config.azure.subscriptionId"));
            properties.put(AzureProperties.CLUSTER_ID.key(), System.getProperty("com.datasphere.config.azure.clusterId"));
            properties.put(AzureProperties.GROUP_NAME.key(), System.getProperty("com.datasphere.config.azure.groupName"));
            final DiscoveryStrategyConfig azureDiscoveryStrategyConfig = new DiscoveryStrategyConfig("com.hazelcast.azure.AzureDiscoveryStrategy", (Map)properties);
            join.getDiscoveryConfig().addDiscoveryStrategyConfig(azureDiscoveryStrategyConfig);
        }
        else if (isTcpIp) {
            printf("Using TcpIp clustering to discover the cluster members\n");
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true);
            final String members = System.getProperty("com.datasphere.config.servernode.address");
            if (members != null) {
                final List<String> memberList = Arrays.asList(members.split(","));
                printf(memberList.toString() + "\n");
                final List<String> resolvedNames = new ArrayList<String>();
                for (final String member : memberList) {
                    try {
                        String host = member;
                        String port = null;
                        if (member.contains(":")) {
                            host = member.split(":")[0];
                            port = member.split(":")[1];
                        }
                        String hostAddress = InetAddress.getByName(host).getHostAddress();
                        if (port != null) {
                            hostAddress = hostAddress + ":" + port;
                        }
                        resolvedNames.add(hostAddress);
                    }
                    catch (UnknownHostException e) {
                        resolvedNames.add(member);
                    }
                }
                printf("Resolved Cluster Members to join " + resolvedNames + "\n");
                join.getTcpIpConfig().setMembers((List)resolvedNames);
            }
            else {
                final List<String> memberList = getMembersFromFile();
                if (memberList != null) {
                    printf(memberList.toString() + "\n");
                }
                else {
                    printf("No member found in file");
                }
                if (memberList != null) {
                    join.getTcpIpConfig().setMembers((List)memberList);
                }
            }
        }
        else {
            final byte[] chosen = intToByteArray(clusterName.hashCode());
            final int part0 = chosen[0] & 0xFF;
            final int part2 = chosen[1] & 0xFF;
            final int part3 = chosen[2] & 0xFF;
            final String multicastGroup = "239." + part3 + "." + part2 + "." + part0;
            final MulticastConfig multicast = join.getMulticastConfig();
            multicast.setMulticastGroup(multicastGroup);
            multicast.setMulticastPort(54327);
            final String boolString = System.getProperty("com.dss.nohzoutput", "false");
            boolean boolVal;
            if (boolString == null) {
                boolVal = false;
            }
            else {
                try {
                    boolVal = Boolean.parseBoolean(boolString);
                }
                catch (Exception e2) {
                    boolVal = false;
                }
            }
            if (boolVal) {
                printf("Using Multicast to discover the cluster members on group " + multicastGroup + " port " + 54327 + "\n");
            }
            join.setMulticastConfig(multicast);
        }
        final String nodePublicIp = System.getProperty("com.datasphere.config.node.public.address");
        if (nodePublicIp != null && !nodePublicIp.isEmpty()) {
            HazelcastSingleton.logger.info((Object)("Setting Public Address of the node as '" + nodePublicIp + "'"));
            network.setPublicAddress(nodePublicIp);
        }
        final InterfacesConfig intf = network.getInterfaces();
        final List<String> ListOfAvailableInterfaces = getInterface();
        HazelcastSingleton.interfaceValuePassedFromSytemProperties = System.getProperty("com.datasphere.config.interface");
        if (HazelcastSingleton.interfaceValuePassedFromSytemProperties != null && HazelcastSingleton.interfaceValuePassedFromSytemProperties.equals("null")) {
            HazelcastSingleton.interfaceValuePassedFromSytemProperties = null;
        }
        if (HazelcastSingleton.interfaceValuePassedFromSytemProperties != null && !HazelcastSingleton.interfaceValuePassedFromSytemProperties.isEmpty()) {
            if (HazelcastSingleton.logger.isInfoEnabled()) {
                HazelcastSingleton.logger.info((Object)("Using interface : " + HazelcastSingleton.interfaceValuePassedFromSytemProperties + " , passed in thorugh system properties"));
            }
            HazelcastSingleton.InterfaceToBind = HazelcastSingleton.interfaceValuePassedFromSytemProperties;
            if (passedInterfaces != null) {
                final String[] pIntf = passedInterfaces.split(",");
                for (int i = 0; i < pIntf.length; ++i) {
                    if (pIntf[i].equals(HazelcastSingleton.InterfaceToBind)) {
                        HazelcastSingleton.passedInterfacesDidNotMatchAvailableInterfaces = false;
                        break;
                    }
                    HazelcastSingleton.passedInterfacesDidNotMatchAvailableInterfaces = true;
                }
            }
        }
        else if (passedInterfaces != null) {
            final String[] pIntf = passedInterfaces.split(",");
            for (int i = 0; i < pIntf.length; ++i) {
                if (ListOfAvailableInterfaces.contains(pIntf[i])) {
                    if (HazelcastSingleton.logger.isInfoEnabled()) {
                        HazelcastSingleton.logger.info((Object)("Using interface :" + pIntf[i] + " to bind"));
                    }
                    HazelcastSingleton.InterfaceToBind = pIntf[i];
                    break;
                }
            }
            if (HazelcastSingleton.InterfaceToBind == null) {
                HazelcastSingleton.passedInterfacesDidNotMatchAvailableInterfaces = true;
                if (HazelcastSingleton.logger.isInfoEnabled()) {
                    HazelcastSingleton.logger.warn((Object)"Interfaces in file did not match available interfaces");
                }
                chooseInterface(ListOfAvailableInterfaces);
            }
        }
        else if (ListOfAvailableInterfaces.isEmpty()) {
            HazelcastSingleton.logger.error((Object)"Did not find any interfaces not even Loopback !!!");
            System.exit(0);
        }
        else if (ListOfAvailableInterfaces.size() == 1) {
            if (HazelcastSingleton.logger.isInfoEnabled()) {
                HazelcastSingleton.logger.info((Object)("Only 1 interface available, Connecting to Interface(does not take into count Loopback) : " + ListOfAvailableInterfaces.get(0)));
            }
            HazelcastSingleton.passedInterfacesDidNotMatchAvailableInterfaces = true;
            HazelcastSingleton.InterfaceToBind = ListOfAvailableInterfaces.get(0);
        }
        else if (ListOfAvailableInterfaces.size() > 1) {
            HazelcastSingleton.passedInterfacesDidNotMatchAvailableInterfaces = true;
            chooseInterface(ListOfAvailableInterfaces);
        }
        final String boolString2 = System.getProperty("com.dss.nohzoutput", "false");
        boolean boolVal2;
        if (boolString2 == null) {
            boolVal2 = false;
        }
        else {
            try {
                boolVal2 = Boolean.parseBoolean(boolString2);
            }
            catch (Exception e3) {
                boolVal2 = false;
            }
        }
        if (boolVal2) {
            printf("Using " + HazelcastSingleton.InterfaceToBind + "\n");
        }
        intf.addInterface(HazelcastSingleton.InterfaceToBind);
        intf.setEnabled(true);
        network.setInterfaces(intf);
        setMacID();
        HazelcastSingleton.config.setNetworkConfig(network);
        HazelcastSingleton.config.getNetworkConfig().setPort(5701);
        HazelcastSingleton.config.getNetworkConfig().setPortAutoIncrement(true);
        if (HazelcastSingleton.logger.isInfoEnabled()) {
            HazelcastSingleton.logger.info((Object)(" Current interface in Configuration " + HazelcastSingleton.config.getNetworkConfig().getInterfaces()));
        }
        HazelcastSingleton.config.getGroupConfig().setName(clusterName);
        String password = "HDClusterPassword";
        try {
            password = HSecurityManager.encrypt(password + clusterName, HSecurityManager.ENCRYPTION_SALT.toEightBytes());
        }
        catch (Throwable t) {}
        HazelcastSingleton.config.getGroupConfig().setPassword(password);
        if (HazelcastSingleton.logger.isInfoEnabled()) {
            HazelcastSingleton.logger.info((Object)("Initializing cluster with name: " + clusterName));
        }
        return HazelcastSingleton.config;
    }
    
    private static ClientConfig createClientConfig(final String clusterName) {
        final ClientConfig clientConfig = new ClientConfig();
        final String isEc2Cluster = System.getProperty("com.datasphere.config.enable-ec2Clustering");
        final Boolean isEc2 = new Boolean(isEc2Cluster != null && isEc2Cluster.equalsIgnoreCase("TRUE"));
        final String isAzureCluster = System.getProperty("com.datasphere.config.enable-azureClustering", "False");
        final Boolean isAzure = new Boolean(isAzureCluster != null && isAzureCluster.equalsIgnoreCase("TRUE"));
        if (isEc2) {
            final ClientAwsConfig clientAwsConfig = new ClientAwsConfig();
            clientAwsConfig.setInsideAws(false);
            if (System.getProperty("com.datasphere.config.enable-ec2AccessKey") != null && System.getProperty("com.datasphere.config.enable-ec2SecretKey") != null && !System.getProperty("com.datasphere.config.enable-ec2AccessKey").isEmpty() && !System.getProperty("com.datasphere.config.enable-ec2SecretKey").isEmpty()) {
                clientAwsConfig.setAccessKey(System.getProperty("com.datasphere.config.enable-ec2AccessKey", System.getenv("DSS_EC2_ACCESS_KEY")));
                clientAwsConfig.setSecretKey(System.getProperty("com.datasphere.config.enable-ec2SecretKey", System.getenv("DSS_EC2_SECRET_KEY")));
            }
            if (System.getProperty("com.datasphere.config.ec2IamRole") != null && !System.getProperty("com.datasphere.config.ec2IamRole").isEmpty()) {
                clientAwsConfig.setIamRole(System.getProperty("com.datasphere.config.ec2IamRole"));
            }
            final String region = System.getProperty("com.datasphere.config.enable-ec2Region", "us-east-1");
            clientAwsConfig.setRegion(region);
            clientAwsConfig.setHostHeader("ec2." + region + ".amazonaws.com");
            clientAwsConfig.setSecurityGroupName(System.getProperty("com.datasphere.config.enable-ec2SecurityGroup"));
            clientAwsConfig.setEnabled(true);
            clientConfig.getNetworkConfig().setAwsConfig(clientAwsConfig);
        }
        else if (isAzure) {
            final Map<String, Comparable> properties = new HashMap<String, Comparable>();
            properties.put(AzureProperties.CLIENT_ID.key(), System.getProperty("com.datasphere.config.azure.clientId"));
            properties.put(AzureProperties.CLIENT_SECRET.key(), System.getProperty("com.datasphere.config.azure.clientSecret"));
            properties.put(AzureProperties.TENANT_ID.key(), System.getProperty("com.datasphere.config.azure.tenantId"));
            properties.put(AzureProperties.SUBSCRIPTION_ID.key(), System.getProperty("com.datasphere.config.azure.subscriptionId"));
            properties.put(AzureProperties.CLUSTER_ID.key(), System.getProperty("com.datasphere.config.azure.clusterId"));
            properties.put(AzureProperties.GROUP_NAME.key(), System.getProperty("com.datasphere.config.azure.groupName"));
            final DiscoveryStrategyConfig azureDiscoveryStrategyConfig = new DiscoveryStrategyConfig("com.hazelcast.azure.AzureDiscoveryStrategy", (Map)properties);
            final DiscoveryConfig discoveryConfig = new DiscoveryConfig();
            discoveryConfig.addDiscoveryStrategyConfig(azureDiscoveryStrategyConfig);
            clientConfig.getNetworkConfig().setDiscoveryConfig(discoveryConfig);
            final String serverNodeAddress = System.getProperty("com.datasphere.config.servernode.address");
            clientConfig.getNetworkConfig().setAddresses((List)Arrays.asList(serverNodeAddress.split(",")));
        }
        else {
            final String serverNodeAddress2 = System.getProperty("com.datasphere.config.servernode.address");
            List<String> serverMembers = new ArrayList<String>();
            if (serverNodeAddress2 == null || serverNodeAddress2.trim().isEmpty()) {
                final UdpDiscoveryClient discoveryClient = new UdpDiscoveryClient(9080, clusterName, HSecurityManager.ENCRYPTION_SALT);
                try {
                    final String discoveredNode = discoveryClient.discoverHDServers().trim();
                    serverMembers.add(discoveredNode);
                    if (discoveredNode == null) {
                        throw new RuntimeException("Failed to discover HD Server in the network. Please specify address using -S option");
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException("Failed to discover HD Server in the network. Please specify address using -S option");
                }
            }
            else {
                serverMembers = Arrays.asList(serverNodeAddress2.split(","));
            }
            clientConfig.getNetworkConfig().setAddresses((List)serverMembers);
        }
        clientConfig.getGroupConfig().setName(clusterName);
        String password = "HDClusterPassword";
        try {
            password = HSecurityManager.encrypt(password + clusterName, HSecurityManager.ENCRYPTION_SALT.toEightBytes());
        }
        catch (Throwable t) {}
        clientConfig.getGroupConfig().setPassword(password);
        clientConfig.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass((Class)UUID.class).setImplementation((Serializer)new CustomHazelcastKryoSerializer(UUID.class, 9900)));
        clientConfig.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass((Class)AuthToken.class).setImplementation((Serializer)new CustomHazelcastKryoSerializer(AuthToken.class, 9901)));
        clientConfig.getSerializationConfig().getSerializerConfigs().add(new SerializerConfig().setTypeClass((Class)SimpleEvent.class).setImplementation((Serializer)new CustomHazelcastKryoSerializer(SimpleEvent.class, 9902)));
        return clientConfig;
    }
    
    private static List<String> getMembersFromFile() {
        final Properties props = null;
        final String filePath = NodeStartUp.getPlatformHome() + "/" + "conf" + "/members.txt";
        final File f = new File(filePath);
        if (f.exists()) {
            try {
                final List<String> members = (List<String>)FileUtils.readLines(f);
                return members;
            }
            catch (IOException | NullPointerException ex2) {
                HazelcastSingleton.logger.error((Object)"Error while reading members file.", (Throwable)ex2);
            }
        }
        return null;
    }
    
    public static boolean isAvailable() {
        return HazelcastSingleton.instance != null;
    }
    
    public static void printf(final String toPrint) {
        synchronized (System.out) {
            if (System.console() != null) {
                System.console().printf(toPrint, new Object[0]);
            }
            else {
                System.out.print(toPrint);
            }
            System.out.flush();
        }
    }
    
    private static MapConfig getMapConfig(final String mapName) {
        final MapConfig mapCfg = new MapConfig();
        mapCfg.setName(mapName);
        mapCfg.setBackupCount(1);
        return mapCfg;
    }
    
    public static void shutdown() {
        if (HazelcastSingleton.instance != null) {
            final LifecycleService srv = HazelcastSingleton.instance.getLifecycleService();
            srv.shutdown();
            HazelcastSingleton.instance = null;
            HazelcastSingleton.nodeId = null;
            HazelcastSingleton.config = null;
            HazelcastSingleton.isFirstMember = -1;
        }
    }
    
    static {
        HazelcastSingleton.logger = Logger.getLogger((Class)HazelcastSingleton.class);
        HazelcastSingleton.instance = null;
        HazelcastSingleton.config = null;
        HazelcastSingleton.nodeId = null;
        HazelcastSingleton.InterfaceToBind = null;
        HazelcastSingleton.passedInterfacesDidNotMatchAvailableInterfaces = false;
        HazelcastSingleton.interfaceValuePassedFromSytemProperties = null;
        HazelcastSingleton.isFirstMember = -1;
        HazelcastSingleton.activeClusterMembers = null;
        HazelcastSingleton.noClusterJoin = false;
    }
    
    public enum HDClusterMemberType
    {
        SERVERNODE, 
        AGENTNODE, 
        CONSOLENODE;
    }
    
    static class TungstenProperties
    {
        static int TIMEOUT_SEC;
        static int TIMEOUT_NUM;
        
        static {
            TungstenProperties.TIMEOUT_SEC = 3000;
            TungstenProperties.TIMEOUT_NUM = 3;
        }
    }
}
