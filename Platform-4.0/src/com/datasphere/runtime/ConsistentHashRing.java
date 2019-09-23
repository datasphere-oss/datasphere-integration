package com.datasphere.runtime;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.metaRepository.*;
import java.net.*;

import com.datasphere.distribution.*;
import com.hazelcast.core.*;
import com.datasphere.runtime.exceptions.*;
import com.datasphere.runtime.utils.*;

import java.util.*;
import java.io.*;

public class ConsistentHashRing
{
    private static Logger logger;
    private static final int NULL_HASH_CODE;
    private static final int numberOfPieces = 503;
    private final int requestedNumberOfReplicas;
    private int numberOfReplicas;
    private final int numberOfPartitions;
    private final String name;
    private final TreeMap<Integer, UUID> ring;
    private final List<UUID> currentNodes;
    private final UUID thisId;
    private final Map<UUID, String> addrCache;
    private UUID[][] currentParts;
    private List<UpdateListener> listeners;
    private boolean shuttingDown;
//    static final /* synthetic */ boolean $assertionsDisabled;
    
    public int murmur(final byte[] data, final int seed) {
        final int m = 1540483477;
        final int r = 24;
        int h = seed ^ data.length;
        final int len = data.length;
        final int len_4 = len >> 2;
        for (int i = 0; i < len_4; ++i) {
            final int i_4 = i << 2;
            int k = data[i_4 + 3];
            k <<= 8;
            k |= (data[i_4 + 2] & 0xFF);
            k <<= 8;
            k |= (data[i_4 + 1] & 0xFF);
            k <<= 8;
            k |= (data[i_4 + 0] & 0xFF);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        final int len_m = len_4 << 2;
        final int left = len - len_m;
        if (left != 0) {
            if (left >= 3) {
                h ^= data[len - 3] << 16;
            }
            if (left >= 2) {
                h ^= data[len - 2] << 8;
            }
            if (left >= 1) {
                h ^= data[len - 1];
            }
            h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }
    
    public int goldenratio32(final int i) {
        return i * -1640531527;
    }
    
    public int hash(final Object obj) {
        if (obj instanceof String) {
            return this.murmur(((String)obj).getBytes(), 503);
        }
        return this.goldenratio32(obj.hashCode());
    }
    
    private List<Pair<String, String>> getNode2IpMap() {
        final List<Pair<String, String>> ret = new ArrayList<Pair<String, String>>();
        final HazelcastInstance hz = HazelcastSingleton.get();
        for (final Member m : hz.getCluster().getMembers()) {
            final String uuid = m.getUuid();
            final InetSocketAddress a = m.getSocketAddress();
            final String ip = a.getAddress().getHostAddress() + ":" + a.getPort();
            ret.add(Pair.make(uuid, ip));
        }
        return ret;
    }
    
    private String idToAddress(final UUID uuid) throws NodeNotFoundException {
        String addr = this.addrCache.get(uuid);
        if (addr != null) {
            return addr;
        }
        Endpoint member = HazelcastSingleton.getActiveMembersMap().get(uuid);
        if (member == null) {
            final HazelcastInstance hz = HazelcastSingleton.get();
            for (final Member m : hz.getCluster().getMembers()) {
                if (m.getUuid().equals(uuid.toString())) {
                    member = (Endpoint)m;
                    break;
                }
            }
        }
        if (member != null) {
            InetSocketAddress a = null;
            if (member instanceof Member) {
                a = ((Member)member).getSocketAddress();
            }
            else if (member instanceof Client) {
                a = ((Client)member).getSocketAddress();
            }
            addr = a.getAddress().getHostAddress() + ":" + a.getPort();
            this.addrCache.put(uuid, addr);
            return addr;
        }
        if (HazelcastSingleton.isClientMember()) {
            return uuid.toString();
        }
        throw new NodeNotFoundException("cannot map node ID [" + uuid + "] to socket address\n" + this.getNode2IpMap());
    }
    
    public ConsistentHashRing() {
        this("HashRing-" + System.currentTimeMillis(), 1);
    }
    
    public ConsistentHashRing(final String name, final int requestedNumberOfReplicas) {
        this(name, requestedNumberOfReplicas, 1023);
    }
    
    public ConsistentHashRing(final String name, final int requestedNumberOfReplicas, final int numberOfPartitions) {
        this.ring = new TreeMap<Integer, UUID>();
        this.currentNodes = new ArrayList<UUID>();
        this.thisId = HazelcastSingleton.getNodeId();
        this.addrCache = Factory.makeMap();
        this.currentParts = null;
        this.listeners = new ArrayList<UpdateListener>();
        this.shuttingDown = false;
        this.requestedNumberOfReplicas = requestedNumberOfReplicas;
        this.numberOfReplicas = 0;
        this.name = name;
        this.numberOfPartitions = numberOfPartitions;
        if (ConsistentHashRing.logger.isDebugEnabled()) {
            ConsistentHashRing.logger.debug((Object)(name + ": Created Partitioning scheme with " + this.numberOfReplicas + " replicas"));
        }
    }
    
    public String getName() {
        return this.name;
    }
    
    public UUID[][] getPartitionMap() {
        if (this.requestedNumberOfReplicas == 0) {
            this.numberOfReplicas = this.currentNodes.size();
        }
        else {
            this.numberOfReplicas = this.requestedNumberOfReplicas;
        }
        if (this.numberOfReplicas == 0) {
            return null;
        }
        final UUID[][] parts = new UUID[this.numberOfPartitions][];
        synchronized (this.ring) {
            for (int p = 0; p < this.numberOfPartitions; ++p) {
                parts[p] = new UUID[this.numberOfReplicas];
                final int midValue = p * (Integer.MAX_VALUE / this.numberOfPartitions) + Integer.MAX_VALUE / (this.numberOfPartitions * 2);
                final List<UUID> uuids = this.getN(midValue, this.numberOfReplicas);
                assert uuids != null;
                for (int b = 0; b < this.numberOfReplicas; ++b) {
                    parts[p][b] = uuids.get((b < uuids.size()) ? b : (uuids.size() - 1));
                }
            }
        }
        return parts;
    }
    
    public int getNumberOfPartitions() {
        return this.numberOfPartitions;
    }
    
    public List<Integer> getPartitionsForNode(final UUID uuid) {
        final List<Integer> partitions = new ArrayList<Integer>();
        for (final Map.Entry<Integer, UUID> entry : this.ring.entrySet()) {
            if (entry.getValue().equals((Object)uuid)) {
                partitions.add(entry.getKey());
            }
        }
        return partitions;
    }
    
    public boolean isMultiNode() {
        return this.currentNodes.size() > 1;
    }
    
    public List<UUID> getNodes() {
        return this.currentNodes;
    }
    
    public int size() {
        return this.currentNodes.size();
    }
    
    public int getNumReplicas() {
        return this.numberOfReplicas;
    }
    
    public boolean isFullyReplicated() {
        return this.requestedNumberOfReplicas == 0;
    }
    
    public void addUpdateListener(final UpdateListener listener) {
        synchronized (this.listeners) {
            this.listeners.add(listener);
        }
    }
    
    public void removeUpdateListener(final UpdateListener listener) {
        synchronized (this.listeners) {
            this.listeners.remove(listener);
        }
    }
    
    private void updateStart() {
        if (ConsistentHashRing.logger.isInfoEnabled()) {
            ConsistentHashRing.logger.info((Object)(this.name + ": updating start listeners"));
        }
        for (final UpdateListener listener : this.listeners) {
            listener.updateStart();
        }
        if (ConsistentHashRing.logger.isInfoEnabled()) {
            ConsistentHashRing.logger.info((Object)(this.name + ": done updating start listeners"));
        }
    }
    
    private void updateEnd() {
        if (ConsistentHashRing.logger.isInfoEnabled()) {
            ConsistentHashRing.logger.info((Object)(this.name + ": updating end listeners"));
        }
        for (final UpdateListener listener : this.listeners) {
            listener.updateEnd();
        }
        if (ConsistentHashRing.logger.isInfoEnabled()) {
            ConsistentHashRing.logger.info((Object)(this.name + ": done updating end listeners"));
        }
    }
    
    private void update(final UUID from, final UUID to, final int partId) {
        final Update update = new Update(from, to, partId);
        for (final UpdateListener listener : this.listeners) {
            listener.update(update);
        }
    }
    
    public void showDistribution() {
        if (this.currentNodes.size() == 0) {
            return;
        }
        if (ConsistentHashRing.logger.isDebugEnabled()) {
            ConsistentHashRing.logger.debug((Object)(this.name + " Distribution"));
            for (int b = 0; b < this.numberOfReplicas; ++b) {
                ConsistentHashRing.logger.debug((Object)("  " + ((b == 0) ? "Primary" : ("Backup " + b)) + " Partitions"));
                final Map<UUID, Integer> counts = new TreeMap<UUID, Integer>();
                for (int p = 0; p < this.numberOfPartitions; ++p) {
                    final UUID uuid = this.currentParts[p][b];
                    final Integer currentCount = counts.get(uuid);
                    counts.put(uuid, (currentCount == null) ? 1 : (currentCount + 1));
                }
                for (final Map.Entry<UUID, Integer> count : counts.entrySet()) {
                    ConsistentHashRing.logger.debug((Object)("    " + count.getKey() + " = " + count.getValue()));
                }
            }
        }
    }
    
    public void shutDown() {
        this.shuttingDown = true;
    }
    
    public void repartition() {
        if (ConsistentHashRing.logger.isInfoEnabled()) {
            ConsistentHashRing.logger.info((Object)(this.name + ": start Repartitioning data"));
        }
        if (this.ring.isEmpty()) {
            this.currentParts = null;
            if (ConsistentHashRing.logger.isInfoEnabled()) {
                ConsistentHashRing.logger.info((Object)(this.name + ": done Repartitioning data"));
            }
            return;
        }
        if (this.shuttingDown) {
            return;
        }
        synchronized (this.ring) {
            this.updateStart();
            final UUID[][] newParts = this.getPartitionMap();
            int numMovements = 0;
            if (this.currentParts != null && newParts != null) {
                if (ConsistentHashRing.logger.isInfoEnabled()) {
                    ConsistentHashRing.logger.info((Object)(this.name + ": Repartitioning data"));
                }
                int oldPercent = 0;
                for (int p = 0; p < this.numberOfPartitions; ++p) {
                    final UUID[] oldP = this.currentParts[p];
                    final UUID[] newP = newParts[p];
                    final List<UUID> oldPList = Arrays.asList(oldP);
                    final List<UUID> newPList = Arrays.asList(newP);
                    for (int iNew = 0; iNew < newP.length; ++iNew) {
                        if (!oldPList.contains(newP[iNew])) {
                            int iOld = 0;
                            while (iOld < oldP.length) {
                                if (this.currentNodes.contains(oldP[iOld])) {
                                    if (this.thisId.equals((Object)oldP[iOld])) {
                                        this.update(oldP[iOld], newP[iNew], p);
                                        ++numMovements;
                                        break;
                                    }
                                    break;
                                }
                                else {
                                    ++iOld;
                                }
                            }
                        }
                    }
                    this.currentParts[p] = newParts[p];
                    final int percent = p * 100 / this.numberOfPartitions;
                    if (percent % 20 == 0 && percent != oldPercent) {
                        if (ConsistentHashRing.logger.isDebugEnabled()) {
                            ConsistentHashRing.logger.debug((Object)("  " + this.name + ": Repartitioning data: " + percent + "%"));
                        }
                        oldPercent = percent;
                    }
                }
                if (ConsistentHashRing.logger.isInfoEnabled()) {
                    ConsistentHashRing.logger.info((Object)(this.name + ": Repartitioning complete with " + numMovements + " movements"));
                }
            }
            else {
                assert newParts != null;
                this.currentParts = newParts;
                if (ConsistentHashRing.logger.isInfoEnabled()) {
                    ConsistentHashRing.logger.info((Object)(this.name + ": done Repartitioning data"));
                }
            }
            this.updateEnd();
        }
        this.showDistribution();
    }
    
    public int getPartitionId(final Object key) {
        if (!(key instanceof Partitionable)) {
            return Math.abs(((key == null) ? ConsistentHashRing.NULL_HASH_CODE : key.hashCode()) % this.numberOfPartitions);
        }
        final Partitionable p = (Partitionable)key;
        if (p.usePartitionId()) {
            return p.getPartitionId();
        }
        final int hashCode = (p.getPartitionKey() == null) ? ConsistentHashRing.NULL_HASH_CODE : p.getPartitionKey().hashCode();
        return Math.abs(hashCode % this.numberOfPartitions);
    }
    
    public UUID getUUIDForKey(final Object key, final int n) {
        final int partId = this.getPartitionId(key);
        return this.getUUIDForPartition(partId, n);
    }
    
    public UUID getUUIDForPartition(final int partId, final int n) {
        synchronized (this.ring) {
            if (n >= this.numberOfReplicas) {
                throw new IllegalArgumentException("Cannot get a backup partition greater than number of backups: " + n + " >= " + this.numberOfReplicas);
            }
            if (partId >= this.numberOfPartitions) {
                throw new IllegalArgumentException("Cannot access a partition greater than number of partitions: " + partId + " >= " + this.numberOfPartitions);
            }
            if (this.currentParts == null || this.currentParts[partId] == null) {
                return this.thisId;
            }
            final UUID[] prt = this.currentParts[partId];
            final UUID partuuid = prt[n];
            return partuuid;
        }
    }
    
    private static void checkPart(final UUID[] part) {
        assert part != null;
        for (final UUID id : part) {
            if (id == null && id == null) {
                throw new AssertionError();
            }
        }
    }
    
    private static void checkPartsTable(final UUID[][] table) {
        assert table != null;
        for (int n = table.length, i = 0; i < n; ++i) {
            checkPart(table[i]);
        }
    }
    
    private void addNode(final UUID node) throws NodeNotFoundException {
        final String nodeId = this.idToAddress(node);
        synchronized (this.ring) {
            for (int i = 0; i < 503; ++i) {
                this.ring.put(this.hash(nodeId + i), node);
            }
            this.currentNodes.add(node);
            final Set<UUID> allNodes = new HashSet<UUID>();
            for (final Map.Entry<Integer, UUID> entry : this.ring.entrySet()) {
                allNodes.add(entry.getValue());
            }
            if (ConsistentHashRing.logger.isInfoEnabled()) {
                ConsistentHashRing.logger.info((Object)(this.name + ": Added node: " + node + " now " + this.currentNodes));
            }
        }
    }
    
    public void add(final UUID node) throws NodeNotFoundException {
        this.addNode(node);
        this.repartition();
    }
    
    private void removeNode(final UUID node) throws NodeNotFoundException {
        final String nodeId = this.idToAddress(node);
        synchronized (this.ring) {
            for (int i = 0; i < 503; ++i) {
                this.ring.remove(this.hash(nodeId + i));
            }
            this.currentNodes.remove(node);
            final Set<UUID> allNodes = new HashSet<UUID>();
            for (final Map.Entry<Integer, UUID> entry : this.ring.entrySet()) {
                allNodes.add(entry.getValue());
            }
            if (ConsistentHashRing.logger.isInfoEnabled()) {
                ConsistentHashRing.logger.info((Object)(this.name + ": Removed node: " + node + " now " + this.currentNodes));
            }
        }
    }
    
    public void remove(final UUID node) throws NodeNotFoundException {
        this.removeNode(node);
        this.repartition();
    }
    
    public void set(final List<UUID> nodes) {
        final List<UUID> toAdd = new ArrayList<UUID>();
        final List<UUID> toRemove = new ArrayList<UUID>();
        synchronized (this.ring) {
            if (nodes != null && !nodes.isEmpty()) {
                for (final UUID node : nodes) {
                    if (!this.currentNodes.contains(node)) {
                        toAdd.add(node);
                    }
                }
                for (final UUID node : this.currentNodes) {
                    if (!nodes.contains(node)) {
                        toRemove.add(node);
                    }
                }
            }
            else {
                toRemove.addAll(this.currentNodes);
            }
            for (final UUID node : toAdd) {
                try {
                    this.addNode(node);
                }
                catch (NodeNotFoundException ex) {}
            }
            for (final UUID node : toRemove) {
                try {
                    this.removeNode(node);
                }
                catch (NodeNotFoundException ex2) {}
            }
            if (toAdd.size() > 0 || toRemove.size() > 0) {
                this.repartition();
            }
        }
    }
    
    public UUID get(final Object key) {
        if (this.ring.isEmpty()) {
            return null;
        }
        int hash = this.hash(key);
        final SortedMap<Integer, UUID> tailMap = this.ring.tailMap(hash);
        hash = (tailMap.isEmpty() ? this.ring.firstKey() : ((int)tailMap.firstKey()));
        return this.ring.get(hash);
    }
    
    public UUID getNext(final Object key) {
        if (this.ring.isEmpty()) {
            return null;
        }
        final UUID first = this.get(key);
        int origHash;
        int hash = origHash = this.hash(key);
        UUID next = first;
        for (boolean loopOne = true; hash != origHash || loopOne; origHash = hash, hash = -1, loopOne = false) {
            final Map.Entry<Integer, UUID> higher = this.ring.higherEntry(hash);
            if (higher != null) {
                hash = higher.getKey();
                final UUID test = higher.getValue();
                if (!first.equals((Object)test)) {
                    next = test;
                    break;
                }
            }
            else {
                final Map.Entry<Integer, UUID> firstEntry = this.ring.firstEntry();
                hash = firstEntry.getKey();
                final UUID test2 = firstEntry.getValue();
                if (!first.equals((Object)test2)) {
                    next = test2;
                    break;
                }
            }
            if (loopOne) {}
        }
        return next;
    }
    
    private Map.Entry<Integer, UUID> nextRingEntry(final int hash) {
        Map.Entry<Integer, UUID> next = this.ring.higherEntry(hash);
        if (next == null) {
            next = this.ring.firstEntry();
        }
        return next;
    }
    
    public List<UUID> getN(final Object key, final int n) {
        if (this.ring.isEmpty()) {
            return null;
        }
        final List<UUID> nUUIDs = new ArrayList<UUID>();
        final UUID first = this.get(key);
        nUUIDs.add(first);
        for (int i = 1; i < n; ++i) {
            int hash = this.hash(key);
            Map.Entry<Integer, UUID> next = this.nextRingEntry(hash);
            final int firstHash;
            hash = (firstHash = next.getKey());
            do {
                final UUID test = next.getValue();
                if (!nUUIDs.contains(test)) {
                    nUUIDs.add(test);
                    break;
                }
                next = this.nextRingEntry(hash);
                hash = next.getKey();
            } while (hash != firstHash);
        }
        return nUUIDs;
    }
    
    static {
        ConsistentHashRing.logger = Logger.getLogger((Class)ConsistentHashRing.class);
        NULL_HASH_CODE = "NULL".hashCode();
    }
    
    public static class Update implements Serializable
    {
        private static final long serialVersionUID = 2814082967693009878L;
        public UUID from;
        public UUID to;
        public int partId;
        
        public Update() {
        }
        
        public Update(final UUID from, final UUID to, final int partId) {
            this.from = from;
            this.to = to;
            this.partId = partId;
        }
        
        @Override
        public String toString() {
            return "Repartition from " + this.from + " to " + this.to + " partition " + this.partId;
        }
    }
    
    public interface UpdateListener
    {
        void updateStart();
        
        void update(final Update p0);
        
        void updateEnd();
    }
}
