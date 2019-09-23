package com.datasphere.utility;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Logger;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;

public class GraphUtility
{
    private static Logger logger;
    
    public static Map<UUID, Integer> calculateInDegree(final Map<UUID, Set<UUID>> graph) {
        final Map<UUID, Integer> inDegrees = new HashMap<UUID, Integer>();
        for (final Map.Entry<UUID, Set<UUID>> graphEntry : graph.entrySet()) {
            inDegrees.put(graphEntry.getKey(), 0);
        }
        for (final Map.Entry<UUID, Set<UUID>> graphEntry : graph.entrySet()) {
            for (final UUID uuidOfNeigborVertex : graphEntry.getValue()) {
                final Integer n = inDegrees.get(uuidOfNeigborVertex);
                inDegrees.put(uuidOfNeigborVertex, ((n == null) ? 0 : n) + 1);
            }
        }
        return inDegrees;
    }
    
    public static Map<UUID, Integer> calculateOutDegree(final Map<UUID, Set<UUID>> graph) {
        final Map<UUID, Integer> outDegrees = new HashMap<UUID, Integer>();
        for (final Map.Entry<UUID, Set<UUID>> graphEntry : graph.entrySet()) {
            outDegrees.put(graphEntry.getKey(), graphEntry.getValue().size());
        }
        return outDegrees;
    }
    
    public static List<UUID> topologicalSort(final Map<UUID, Set<UUID>> graph) throws Exception {
        final Map<UUID, Integer> inDegrees = calculateInDegree(graph);
        final List<UUID> resultSet = new ArrayList<UUID>();
        final ArrayDeque<UUID> nodesWithNoIncomingEdges = new ArrayDeque<UUID>();
        for (final Map.Entry<UUID, Integer> entry : inDegrees.entrySet()) {
            if (entry.getValue() == 0) {
                nodesWithNoIncomingEdges.add(entry.getKey());
            }
        }
        while (!nodesWithNoIncomingEdges.isEmpty()) {
            final UUID node = nodesWithNoIncomingEdges.pop();
            resultSet.add(node);
            for (final UUID adjacentNode : graph.get(node)) {
                inDegrees.put(adjacentNode, inDegrees.get(adjacentNode) - 1);
                if (inDegrees.get(adjacentNode) == 0) {
                    nodesWithNoIncomingEdges.push(adjacentNode);
                }
            }
        }
        for (final Map.Entry<UUID, Integer> entry : inDegrees.entrySet()) {
            if (entry.getValue() != 0) {
                throw new Exception("Cycle in graph");
            }
        }
        return resultSet;
    }
    
    public static Set<UUID> nodesWithNoIncomingEdges(final Map<UUID, Set<UUID>> graph) throws Exception {
        final Map<UUID, Integer> inDegrees = calculateInDegree(graph);
        final List<UUID> resultSet = new ArrayList<UUID>();
        final Set<UUID> nodesWithNoIncomingEdges = new HashSet<UUID>();
        for (final Map.Entry<UUID, Integer> entry : inDegrees.entrySet()) {
            if (entry.getValue() == 0) {
                nodesWithNoIncomingEdges.add(entry.getKey());
            }
        }
        return nodesWithNoIncomingEdges;
    }
    
    public static Set<UUID> nodesWithNoOutgoingEdges(final Map<UUID, Set<UUID>> graph) throws Exception {
        final Map<UUID, Integer> outDegrees = calculateOutDegree(graph);
        final List<UUID> resultSet = new ArrayList<UUID>();
        final Set<UUID> nodesWithNoOutgoingEdges = new HashSet<UUID>();
        for (final Map.Entry<UUID, Integer> entry : outDegrees.entrySet()) {
            if (entry.getValue() == 0) {
                nodesWithNoOutgoingEdges.add(entry.getKey());
            }
        }
        return nodesWithNoOutgoingEdges;
    }
    
    public static Logger getLogger() {
        return GraphUtility.logger;
    }
    
    public static void setLogger(final Logger logger) {
        GraphUtility.logger = logger;
    }
    
    public static void findAllPaths(final Map<UUID, Set<UUID>> graph, final int sizeOfGraph, final UUID start, final UUID destination) {
        final Map<UUID, Boolean> isVisited = (Map<UUID, Boolean>)new HashedMap();
        final ArrayList<UUID> pathList = new ArrayList<UUID>();
        pathList.add(start);
        printAllPathsUtil(graph, start, destination, isVisited, pathList);
    }
    
    private static void printAllPathsUtil(final Map<UUID, Set<UUID>> graph, final UUID u, final UUID d, final Map<UUID, Boolean> isVisited, final List<UUID> localPathList) {
        isVisited.put(u, true);
        if (u.equals((Object)d)) {
            printPath(localPathList);
        }
        for (final UUID i : graph.get(u)) {
            if (!isVisited.containsKey(i)) {
                isVisited.put(i, false);
            }
            if (!isVisited.get(i)) {
                localPathList.add(i);
                printAllPathsUtil(graph, i, d, isVisited, localPathList);
                localPathList.remove(i);
            }
        }
        isVisited.put(u, false);
    }
    
    private static void printPath(final List<UUID> localPathList) {
        final StringBuffer sb = new StringBuffer();
        sb.append("------------------------------------------------------------");
        sb.append("\n{");
        boolean isFirst = false;
        for (final UUID uuid : localPathList) {
            try {
                if (isFirst) {
                    sb.append(" -> ");
                }
                final MetaInfo.MetaObject ob = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, HSecurityManager.TOKEN);
                sb.append(ob.getFQN());
                isFirst = true;
            }
            catch (MetaDataRepositoryException e) {
                e.printStackTrace();
            }
        }
        sb.append("}\n");
        sb.append("------------------------------------------------------------");
        sb.append("\n\n");
        System.out.println(sb.toString());
    }
    
    static {
        GraphUtility.logger = Logger.getLogger((Class)GraphUtility.class);
    }
}
