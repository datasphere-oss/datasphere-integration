package com.datasphere.recovery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class Position implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = 539311015135199997L;
    private static Logger logger;
    private HashMap<Integer, Path> paths;
    
    public Position() {
        this.paths = new HashMap<Integer, Path>();
    }
    
    protected Position(final Position that) {
        this.paths = new HashMap<Integer, Path>();
        this.paths = that.paths;
    }
    
    public Position(final Set<Path> paths) {
        this.paths = new HashMap<Integer, Path>();
        if (paths != null) {
            for (final Path path : paths) {
                this.paths.put(path.getPathHash(), path);
            }
        }
    }
    
    public Position(final Path path) {
        this.paths = new HashMap<Integer, Path>();
        if (path != null) {
            this.paths.put(path.getPathHash(), path);
        }
    }
    
    public Position(final List<Path> paths) {
        this.paths = new HashMap<Integer, Path>();
        if (paths != null) {
            for (final Path path : paths) {
                this.paths.put(path.getPathHash(), path);
            }
        }
    }
    
    public Set<Integer> keySet() {
        return this.paths.keySet();
    }
    
    public Path get(final Integer pathHash) {
        return this.paths.get(pathHash);
    }
    
    public boolean containsKey(final int pathHash) {
        return this.paths.containsKey(pathHash);
    }
    
    public int size() {
        return this.paths.size();
    }
    
    public Collection<Path> values() {
        return this.paths.values();
    }
    
    public Position createAugmentedPosition(final UUID componentUuid, final String distributionID) {
        final Path.Item item = Path.Item.get(componentUuid, distributionID);
        final List<Path> augmentedPaths = new ArrayList<Path>();
        for (final Path path : this.paths.values()) {
            final Path augmentedPath = path.copyAugmentedWith(item);
            augmentedPaths.add(augmentedPath);
        }
        final Position result = new Position(augmentedPaths);
        return result;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Position)) {
            return false;
        }
        final Position that = (Position)obj;
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
    
    public SourcePosition getLowPositionForPathSegment(final Path startingSubpath) {
        SourcePosition result = null;
        for (final Path p : this.paths.values()) {
            if (p.startsWith(startingSubpath) && (result == null || p.getLowSourcePosition().compareTo(result) < 0)) {
                result = p.getLowSourcePosition();
            }
        }
        return result;
    }
    
    public boolean isEmpty() {
        return this.paths.isEmpty();
    }
    
    public boolean containsComponent(final UUID uuid) {
        for (final Path path : this.values()) {
            if (path.contains(uuid)) {
                return true;
            }
        }
        return false;
    }
    
    public Position getLowPositionForComponent(final UUID uuid) {
        final PathManager result = new PathManager();
        for (final Path p : this.paths.values()) {
            final Path pathToUuid = p.segmentEndingWithUuid(uuid);
            if (pathToUuid != null && pathToUuid.getLowSourcePosition() != null) {
                result.mergeWiderPath(pathToUuid);
            }
        }
        return result.toPosition();
    }
    
    public SourcePosition getLowSourcePositionForComponent(final UUID uuid) {
        SourcePosition result = null;
        for (final Path p : this.paths.values()) {
            if (p.contains(uuid)) {
                final SourcePosition sp = p.getLowSourcePosition();
                if (sp == null || (result != null && sp.compareTo(result) >= 0)) {
                    continue;
                }
                result = sp;
            }
        }
        return result;
    }
    
    public static Position from(final UUID sourceUUID, final String distributionID, final SourcePosition sourcePosition, final SourcePosition highSourcePosition) {
        final Set<Path> paths = new HashSet<Path>();
        paths.add(new Path(sourceUUID, distributionID, sourcePosition, highSourcePosition));
        final Position result = new Position(paths);
        return result;
    }
    
    @Deprecated
    public static Position from(final UUID sourceUUID, final String distributionID, final SourcePosition sourcePosition) {
        final Set<Path> paths = new HashSet<Path>();
        paths.add(new Path(sourceUUID, distributionID, sourcePosition, sourcePosition));
        final Position result = new Position(paths);
        return result;
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
    
    public void read(final Kryo kryo, final Input input) {
        final int numPaths = input.readInt();
        if (numPaths == 0) {
            this.paths = new HashMap<Integer, Path>();
        }
        for (int c = 0; c < numPaths; ++c) {
            final int numUuids = input.readInt();
            final Path.Item[] items = new Path.Item[numUuids];
            for (int u = 0; u < numUuids; ++u) {
                items[u] = (Path.Item)kryo.readClassAndObject(input);
            }
            final Path.ItemList pathItems = Path.ItemList.get(items);
            final SourcePosition lowSourcePosition = (SourcePosition)kryo.readClassAndObject(input);
            final SourcePosition highSourcePosition = (SourcePosition)kryo.readClassAndObject(input);
            final Path path = new Path(pathItems, lowSourcePosition, highSourcePosition);
            if (this.paths == null) {
                this.paths = new HashMap<Integer, Path>();
            }
            this.paths.put(path.getPathHash(), path);
        }
    }
    
    public void write(final Kryo kryo, final Output output) {
        output.writeInt(this.size());
        for (final Path p : this.values()) {
            output.writeInt(p.pathComponentCount());
            for (int c = 0; c < p.pathComponentCount(); ++c) {
                final Path.Item item = p.getPathComponent(c);
                kryo.writeClassAndObject(output, (Object)item);
            }
            kryo.writeClassAndObject(output, (Object)p.getLowSourcePosition());
            kryo.writeClassAndObject(output, (Object)p.getHighSourcePosition());
        }
    }
    
    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder();
        result.append("[");
        for (final Path p : this.paths.values()) {
            result.append(p);
            result.append(";");
        }
        result.append("]");
        return result.toString();
    }
    
    static {
        Position.logger = Logger.getLogger((Class)Position.class);
    }
}
