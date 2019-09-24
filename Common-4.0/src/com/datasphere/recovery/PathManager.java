package com.datasphere.recovery;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class PathManager implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = 539311015135199997L;
    private static Logger logger;
    private HashMap<Integer, Path> paths;
    
    public PathManager() {
        this.paths = new HashMap<Integer, Path>();
    }
    
    public PathManager(final Collection<Path> paths) {
        this.paths = new HashMap<Integer, Path>();
        if (paths != null) {
            for (final Path path : paths) {
                this.paths.put(path.getPathHash(), path);
            }
        }
    }
    
    public PathManager(final PathManager that) {
        this.paths = new HashMap<Integer, Path>();
        if (that != null) {
            for (final Path path : that.values()) {
                this.paths.put(path.getPathHash(), path);
            }
        }
    }
    
    public PathManager(final Position that) {
        this.paths = new HashMap<Integer, Path>();
        if (that != null) {
            for (final Path path : that.values()) {
                this.paths.put(path.getPathHash(), path);
            }
        }
    }
    
    public synchronized Set<Integer> keySet() {
        return this.paths.keySet();
    }
    
    public synchronized Path get(final Integer pathHash) {
        return this.paths.get(pathHash);
    }
    
    public synchronized boolean containsKey(final int pathHash) {
        return this.paths.containsKey(pathHash);
    }
    
    public synchronized int size() {
        return this.paths.size();
    }
    
    public synchronized Collection<Path> values() {
        return this.paths.values();
    }
    
    public synchronized void mergeWiderPath(final Path thatPath) {
        if (thatPath == null) {
            return;
        }
        assert thatPath != null;
        final Path thisPath = this.get(thatPath.getPathHash());
        if (thisPath == null) {
            this.paths.put(thatPath.getPathHash(), thatPath);
            return;
        }
        if (thisPath.isAfter() && !thatPath.isAfter()) {
            this.paths.put(thatPath.getPathHash(), thatPath);
            return;
        }
        if (thatPath.isAfter() && !thisPath.isAfter()) {
            return;
        }
        final SourcePosition thisLowSourcePosition = thisPath.getLowSourcePosition();
        final SourcePosition thatLowSourcePosition = thatPath.getLowSourcePosition();
        assert thisLowSourcePosition != null;
        assert thatLowSourcePosition != null;
        final SourcePosition lowerLow = (thatLowSourcePosition.compareTo(thisLowSourcePosition) < 0) ? thatLowSourcePosition : thisLowSourcePosition;
        final SourcePosition thisHighSourcePosition = thisPath.getHighSourcePosition();
        final SourcePosition thatHighSourcePosition = thatPath.getHighSourcePosition();
        SourcePosition higherHigh;
        if (thisHighSourcePosition == null) {
            higherHigh = thatHighSourcePosition;
        }
        else if (thatLowSourcePosition == null) {
            higherHigh = thisHighSourcePosition;
        }
        else {
            higherHigh = ((thatHighSourcePosition.compareTo(thisHighSourcePosition) > 0) ? thatHighSourcePosition : thisHighSourcePosition);
        }
        if (lowerLow == thisLowSourcePosition && higherHigh == thisHighSourcePosition) {
            return;
        }
        final Path mergedPath = new Path(thisPath.getPathItems(), lowerLow, higherHigh, thisPath.getAtOrAfter());
        this.paths.put(mergedPath.getPathHash(), mergedPath);
    }
    
    public synchronized void mergeHigherPath(final Path thatPath) {
        if (thatPath == null) {
            return;
        }
        assert thatPath != null;
        final Path thisPath = this.get(thatPath.getPathHash());
        if (thisPath == null) {
            this.paths.put(thatPath.getPathHash(), thatPath);
            return;
        }
        if (thisPath.isAfter() && !thatPath.isAfter()) {
            this.paths.put(thatPath.getPathHash(), thatPath);
            return;
        }
        if (thatPath.isAfter() && !thisPath.isAfter()) {
            return;
        }
        final SourcePosition thisLowSourcePosition = thisPath.getLowSourcePosition();
        final SourcePosition thatLowSourcePosition = thatPath.getLowSourcePosition();
        assert thisLowSourcePosition != null;
        assert thatLowSourcePosition != null;
        final SourcePosition higherLow = (thatLowSourcePosition.compareTo(thisLowSourcePosition) > 0) ? thatLowSourcePosition : thisLowSourcePosition;
        final SourcePosition thisHighSourcePosition = thisPath.getHighSourcePosition();
        final SourcePosition thatHighSourcePosition = thatPath.getHighSourcePosition();
        SourcePosition higherHigh;
        if (thisHighSourcePosition == null) {
            higherHigh = thatHighSourcePosition;
        }
        else if (thatLowSourcePosition == null) {
            higherHigh = thisHighSourcePosition;
        }
        else {
            higherHigh = ((thatHighSourcePosition.compareTo(thisHighSourcePosition) > 0) ? thatHighSourcePosition : thisHighSourcePosition);
        }
        if (higherLow == thisLowSourcePosition && higherHigh == thisHighSourcePosition) {
            return;
        }
        final Path mergedPath = new Path(thisPath.getPathItems(), higherLow, higherHigh, thisPath.getAtOrAfter());
        this.paths.put(mergedPath.getPathHash(), mergedPath);
    }
    
    public synchronized void mergeWiderPosition(final Collection<Path> that) {
        if (that == null || that.isEmpty()) {
            return;
        }
        for (final Path thatPath : that) {
            this.mergeWiderPath(thatPath);
        }
    }
    
    public synchronized void mergeLowerPositions(final Position that) {
        if (that == null || that.isEmpty()) {
            return;
        }
        for (final Integer pathHash : that.keySet()) {
            final Path thatPath = that.get(pathHash);
            final Path thisPath = this.get(pathHash);
            assert thatPath != null : "That path can't be merged because it is null";
            assert thatPath.getLowSourcePosition() != null : "That path can't be merged because it has a null low source position";
            if (this.containsKey(pathHash) && thatPath.getLowSourcePosition().compareTo(thisPath.getLowSourcePosition()) >= 0) {
                continue;
            }
            this.paths.put(thatPath.getPathHash(), thatPath);
        }
    }
    
    public synchronized void mergeLowerPositions(final PathManager that) {
        if (that == null || that.isEmpty()) {
            return;
        }
        for (final Integer pathHash : that.keySet()) {
            final Path thatPath = that.get(pathHash);
            final Path thisPath = this.get(pathHash);
            assert thatPath != null;
            assert thatPath.getLowSourcePosition() != null;
            if (this.containsKey(pathHash) && thatPath.getLowSourcePosition().compareTo(thisPath.getLowSourcePosition()) >= 0) {
                continue;
            }
            this.paths.put(thatPath.getPathHash(), thatPath);
        }
    }
    
    public synchronized void mergeHigherPositions(final Position that) {
        if (that == null || that.isEmpty()) {
            return;
        }
        for (final Integer pathHash : that.keySet()) {
            final Path thatPath = that.get(pathHash);
            final Path thisPath = this.get(pathHash);
            if (thatPath == null) {
                PathManager.logger.error((Object)("Unexpected null path found in merging position: " + ((thisPath == null) ? pathHash : thisPath.toString())));
            }
            else {
                if (this.containsKey(pathHash) && thatPath.getLowSourcePosition().compareTo(thisPath.getLowSourcePosition()) <= 0) {
                    continue;
                }
                this.paths.put(thatPath.getPathHash(), thatPath);
            }
        }
    }
    
    public synchronized void mergeHigherPositions(final PathManager that) {
        if (that == null || that.isEmpty()) {
            return;
        }
        for (final Integer pathHash : that.keySet()) {
            final Path thatPath = that.get(pathHash);
            final Path thisPath = this.get(pathHash);
            if (thatPath == null) {
                PathManager.logger.error((Object)("Unexpected null path found in merging position: " + ((thisPath == null) ? pathHash : thisPath.toString())));
            }
            else {
                if (this.containsKey(pathHash) && thatPath.getLowSourcePosition().compareTo(thisPath.getLowSourcePosition()) <= 0) {
                    continue;
                }
                this.paths.put(thatPath.getPathHash(), thatPath);
            }
        }
    }
    
    public synchronized Position createPositionWithoutPaths(final Set<Integer> withoutThese) {
        final Set<Path> resultPaths = new HashSet<Path>();
        if (withoutThese != null) {
            for (final Path path : this.paths.values()) {
                if (!withoutThese.contains(path.getPathHash())) {
                    resultPaths.add(path);
                }
            }
        }
        final Position result = new Position(resultPaths);
        return result;
    }
    
    @Override
    public synchronized boolean equals(final Object obj) {
        if (!(obj instanceof PathManager)) {
            return false;
        }
        final PathManager that = (PathManager)obj;
        if (this.size() != that.size()) {
            return false;
        }
        for (final Integer key : that.keySet()) {
            if (!this.containsKey(key)) {
                return false;
            }
            final SourcePosition thisSP = this.get(key).getLowSourcePosition();
            final SourcePosition thatSP = that.get(key).getLowSourcePosition();
            final int comp = thisSP.compareTo(thatSP);
            if (comp != 0) {
                return false;
            }
        }
        return true;
    }
    
    public synchronized SourcePosition getLowPositionForPathSegment(final Path startingSubpath) {
        SourcePosition result = null;
        if (startingSubpath != null) {
            for (final Path p : this.paths.values()) {
                if (p.startsWith(startingSubpath) && (result == null || p.getLowSourcePosition().compareTo(result) < 0)) {
                    result = p.getLowSourcePosition();
                }
            }
        }
        return result;
    }
    
    public synchronized boolean isEmpty() {
        return this.paths.isEmpty();
    }
    
    public synchronized void removePathsWhichStartWith(final Collection<Path> removalBasePaths) {
        if (removalBasePaths == null) {
            return;
        }
        final Iterator<Map.Entry<Integer, Path>> thisPathsIter = this.paths.entrySet().iterator();
        while (thisPathsIter.hasNext()) {
            final Map.Entry<Integer, Path> thisEntry = thisPathsIter.next();
            final Path candidateForRemoval = thisEntry.getValue();
            for (final Path removalBasePath : removalBasePaths) {
                if (candidateForRemoval.startsWith(removalBasePath)) {
                    thisPathsIter.remove();
                    break;
                }
            }
        }
    }
    
    public synchronized boolean containsComponent(final UUID uuid) {
        for (final Path path : this.values()) {
            if (path.contains(uuid)) {
                return true;
            }
        }
        return false;
    }
    
    public synchronized PathManager getLowPositionForComponent(final UUID uuid) {
        final PathManager result = new PathManager();
        for (final Path p : this.paths.values()) {
            final Path pathToUuid = p.segmentEndingWithUuid(uuid);
            if (pathToUuid != null && pathToUuid.getLowSourcePosition() != null) {
                result.mergeWiderPath(pathToUuid);
            }
        }
        return result;
    }
    
    public PathManager getLowPositionForComponent(final UUID uuid, final String distId) {
        final PathManager result = new PathManager();
        for (final Path p : this.paths.values()) {
            final Path pathToUuid = p.segmentEndingWithUuid(uuid, distId);
            if (pathToUuid != null && pathToUuid.getLowSourcePosition() != null) {
                result.mergeWiderPath(pathToUuid);
            }
        }
        return result;
    }
    
    public synchronized Path getLowSourcePositionForComponent(final UUID uuid) {
        Path result = null;
        for (final Path p : this.paths.values()) {
            if (p.contains(uuid)) {
                final SourcePosition sp = p.getLowSourcePosition();
                if (sp == null || (result != null && sp.compareTo(result.getLowSourcePosition()) >= 0)) {
                    continue;
                }
                result = p;
            }
        }
        return result;
    }
    
    public synchronized PartitionedSourcePosition getPartitionedSourcePositionForComponent(final UUID uuid) {
        final Map<String, SourcePosition> sourcePositionMap = new HashMap<String, SourcePosition>();
        final Map<String, Boolean> atOrAfterMap = new HashMap<String, Boolean>();
        for (final Path p : this.paths.values()) {
            if (p.contains(uuid)) {
                final String partID = p.getPathComponent(0).getDistributionID();
                final SourcePosition sp = p.getLowSourcePosition();
                if (sourcePositionMap.containsKey(partID) && sp.compareTo((SourcePosition)sourcePositionMap.get(partID)) >= 0) {
                    continue;
                }
                sourcePositionMap.put(partID, sp);
                atOrAfterMap.put(partID, p.getAtOrAfterBoolean());
            }
        }
        final PartitionedSourcePosition result = new PartitionedSourcePosition(sourcePositionMap, atOrAfterMap);
        return result;
    }
    
    public boolean blocks(final Position that) {
        if (that == null) {
            return false;
        }
        for (final Path thatPath : that.values()) {
            for (final Path thisPath : this.paths.values()) {
                if (thisPath.startsWith(thatPath)) {
                    if (thisPath.isAfter()) {
                        return thisPath.getHighSourcePosition() != null && thatPath.getLowSourcePosition().compareTo(thisPath.getHighSourcePosition()) > 0;
                    }
                    return thisPath.getLowSourcePosition().compareTo(thatPath.getLowSourcePosition()) > 0;
                }
            }
        }
        return false;
    }
    
    public boolean equalsOrExceeds(final Position that) {
        if (that == null) {
            return false;
        }
        for (final Path thatPath : that.values()) {
            final SourcePosition thatSP = thatPath.getLowSourcePosition();
            final SourcePosition thisSP = this.getLowPositionForPathSegment(thatPath);
            if (thisSP != null && thisSP.compareTo(thatSP) >= 0) {
                return true;
            }
        }
        return false;
    }
    
    public synchronized void augment(final UUID componentUuid, final String distributionID) {
        final Path.Item item = Path.Item.get(componentUuid, distributionID);
        final Set<Path> augmentedPaths = new HashSet<Path>();
        for (final Path path : this.paths.values()) {
            final Path augmentedPath = path.copyAugmentedWith(item);
            augmentedPaths.add(augmentedPath);
        }
        this.paths.clear();
        for (final Path path : augmentedPaths) {
            this.paths.put(path.getPathHash(), path);
        }
    }
    
    public synchronized int removeEqualPaths(final Position that) {
        if (that == null || that.isEmpty()) {
            return 0;
        }
        final Set<Integer> removeThese = new HashSet<Integer>();
        for (final Path thatPath : that.values()) {
            final Path thisPath = this.get(thatPath.getPathHash());
            if (thisPath != null && thisPath.getLowSourcePosition().equals(thatPath.getLowSourcePosition())) {
                removeThese.add(thatPath.getPathHash());
            }
        }
        for (final Integer removeThis : removeThese) {
            this.paths.remove(removeThis);
        }
        return removeThese.size();
    }
    
    public synchronized void clear() {
        this.paths.clear();
    }
    
    public synchronized int removePaths(final Position that) {
        int result = 0;
        if (that != null) {
            for (final Integer pathHash : that.keySet()) {
                this.paths.remove(pathHash);
                ++result;
            }
        }
        return result;
    }
    
    public synchronized int removePaths(final Set<Integer> pathHashes) {
        int result = 0;
        if (pathHashes != null) {
            for (final Integer pathHash : pathHashes) {
                this.paths.remove(pathHash);
                ++result;
            }
        }
        return result;
    }
    
    public synchronized int removePath(final Integer pathHash) {
        return (this.paths.remove(pathHash) != null) ? 1 : 0;
    }
    
    public void setAtOrAfter(final String atOrAfter) {
        assert "@".equals(atOrAfter) || "^".equals(atOrAfter);
        final Set<Path> newPaths = new HashSet<Path>();
        for (final Path p : this.paths.values()) {
            if (!p.getAtOrAfter().equals(atOrAfter)) {
                newPaths.add(new Path(p.getPathItems(), p.getLowSourcePosition(), p.getHighSourcePosition(), atOrAfter));
            }
        }
        for (final Path newPath : newPaths) {
            this.paths.put(newPath.getPathHash(), newPath);
        }
    }
    
    public synchronized void read(final Kryo kryo, final Input input) {
        for (int numPaths = input.readInt(), c = 0; c < numPaths; ++c) {
            final int numUuids = input.readInt();
            final Path.Item[] items = new Path.Item[numUuids];
            for (int u = 0; u < numUuids; ++u) {
                items[u] = (Path.Item)kryo.readClassAndObject(input);
            }
            final Path.ItemList pathItems = Path.ItemList.get(items);
            final SourcePosition sourcePosition = (SourcePosition)kryo.readClassAndObject(input);
            final Path path = new Path(pathItems, sourcePosition);
            if (this.paths == null) {
                this.paths = new HashMap<Integer, Path>();
            }
            this.paths.put(path.getPathHash(), path);
        }
    }
    
    public synchronized void write(final Kryo kryo, final Output output) {
        output.writeInt(this.size());
        for (final Path p : this.values()) {
            output.writeInt(p.pathComponentCount());
            for (int c = 0; c < p.pathComponentCount(); ++c) {
                final Path.Item item = p.getPathComponent(c);
                kryo.writeClassAndObject(output, (Object)item);
            }
            kryo.writeClassAndObject(output, (Object)p.getLowSourcePosition());
        }
    }
    
    @Override
    public synchronized String toString() {
        final StringBuilder result = new StringBuilder();
        result.append("[");
        for (final Path p : this.paths.values()) {
            result.append(p).append(";");
        }
        result.append("]");
        return result.toString();
    }
    
    public synchronized Position toPosition() {
        return new Position(new HashSet<Path>(this.paths.values()));
    }
    
    static {
        PathManager.logger = Logger.getLogger((Class)PathManager.class);
    }
}
