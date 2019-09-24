package com.datasphere.recovery;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Logger;

import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.JsonNode;

public class Path implements Serializable, Comparable<Path>
{
    private static final long serialVersionUID = 1680308749357411041L;
    private static Logger logger;
    public static final String AT = "@";
    public static final String AFTER = "^";
    public static final boolean AT_BOOLEAN = false;
    public static final boolean AFTER_BOOLEAN = true;
    public static final String AT_OR_AFTER_PATTERN = "[@^]";
    public int id;
    public UUID applicationUuid;
    private final ItemList pathItems;
    private final SourcePosition lowSourcePosition;
    private final SourcePosition highSourcePosition;
    private final String atOrAfter;
    
    private Path() {
        this.id = 0;
        this.applicationUuid = null;
        this.pathItems = null;
        this.lowSourcePosition = null;
        this.highSourcePosition = null;
        this.atOrAfter = null;
    }
    
    public Path(final UUID sourceUUID, final String distributionID, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition) {
        this.id = 0;
        this.applicationUuid = null;
        assert sourceUUID != null : "Paths with null source UUIDs are invalid";
        assert lowSourcePosition != null : "Paths with null low source positions are invalid";
        final Item[] items = { Item.get(sourceUUID, distributionID) };
        this.pathItems = ItemList.get(items);
        this.lowSourcePosition = lowSourcePosition;
        this.highSourcePosition = highSourcePosition;
        this.atOrAfter = "@";
    }
    
    public Path(final UUID sourceUUID, final String distributionID, final SourcePosition lowSourcePosition) {
        this(sourceUUID, distributionID, lowSourcePosition, null);
    }
    
    public Path(final ItemList pathItems, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition) {
        this.id = 0;
        this.applicationUuid = null;
        this.pathItems = pathItems;
        this.lowSourcePosition = lowSourcePosition;
        this.highSourcePosition = highSourcePosition;
        this.atOrAfter = "@";
    }
    
    public Path(final ItemList pathItems, final SourcePosition lowSourcePosition) {
        this.id = 0;
        this.applicationUuid = null;
        this.pathItems = pathItems;
        this.lowSourcePosition = lowSourcePosition;
        this.highSourcePosition = lowSourcePosition;
        this.atOrAfter = "@";
    }
    
    public Path(final ItemList pathItems, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition, final boolean atOrAfter) {
        this.id = 0;
        this.applicationUuid = null;
        this.pathItems = pathItems;
        this.lowSourcePosition = lowSourcePosition;
        this.highSourcePosition = highSourcePosition;
        this.atOrAfter = (atOrAfter ? "^" : "@");
    }
    
    public Path(final ItemList pathItems, final SourcePosition lowSourcePosition, final SourcePosition highSourcePosition, final String atOrAfter) {
        this.id = 0;
        this.applicationUuid = null;
        this.pathItems = pathItems;
        this.lowSourcePosition = lowSourcePosition;
        this.highSourcePosition = highSourcePosition;
        this.atOrAfter = ((atOrAfter == "@") ? "@" : "^");
    }
    
    public Path copyAugmentedWith(final Item item) {
        final ItemList augmentedItemList = ItemList.get(this.pathItems.list, item);
        final Path result = new Path(augmentedItemList, this.lowSourcePosition, this.highSourcePosition);
        return result;
    }
    
    public SourcePosition getLowSourcePosition() {
        return this.lowSourcePosition;
    }
    
    public SourcePosition getHighSourcePosition() {
        return this.highSourcePosition;
    }
    
    public int pathComponentCount() {
        return this.pathItems.size();
    }
    
    public Item getPathComponent(final int index) {
        return this.pathItems.get(index);
    }
    
    public ItemList getPathItems() {
        return this.pathItems;
    }
    
    public int getPathHash() {
        return this.pathItems.hashCode();
    }
    
    public boolean contains(final UUID uuid) {
        for (final Item candidateItem : this.pathItems.list) {
            if (candidateItem.componentUuid.equals(uuid)) {
                return true;
            }
        }
        return false;
    }
    
    public boolean contains(final UUID uuid, final String distributionID) {
        for (final Item candidateItem : this.pathItems.list) {
            if (candidateItem.componentUuid.equals(uuid) && candidateItem.distributionID.equals(distributionID)) {
                return true;
            }
        }
        return false;
    }
    
    public boolean contains(final Item item) {
        for (final Item candidate : this.pathItems.list) {
            if (candidate.equals(item)) {
                return true;
            }
        }
        return false;
    }
    
    public boolean startsWith(final Path path) {
        if (path.size() > this.size()) {
            return false;
        }
        for (int i = 0; i < path.size(); ++i) {
            if (!this.getPathComponent(i).equals(path.getPathComponent(i))) {
                return false;
            }
        }
        return true;
    }
    
    private int size() {
        return this.pathItems.size();
    }
    
    public String toJson() {
        try {
            final String lowSpBase64 = this.getBase64EncodedLowSourcePosition();
            final String highSpString = this.getBase64EncodedHighSourcePosition(false);
            return "{ flowUuid:" + this.applicationUuid + " pathItems:" + this.pathItems + " atOrAfter:" + this.atOrAfter + " lowSourcePosition:" + lowSpBase64 + highSpString + " }";
        }
        catch (IOException e) {
            Path.logger.error((Object)e);
            e.printStackTrace();
            return null;
        }
    }
    
    public String toStandardJSON() {
        try {
            String lowSpBase64 = this.getBase64EncodedLowSourcePosition();
            lowSpBase64 = ((lowSpBase64 != null) ? ("\"" + lowSpBase64 + "\"") : null);
            String highSpString = this.getBase64EncodedHighSourcePosition(true);
            highSpString = ((highSpString != null) ? ("\"" + highSpString + "\"") : null);
            final String applicationUuid = (this.applicationUuid != null) ? ("\"" + this.applicationUuid + "\"") : null;
            final String pathItems = (this.pathItems != null) ? ("\"" + this.pathItems + "\"") : null;
            final String atOrAfter = (this.atOrAfter != null) ? ("\"" + this.atOrAfter + "\"") : null;
            return "{\"flowUuid\":" + applicationUuid + ",\"pathItems\":" + pathItems + ",\"atOrAfter\":" + atOrAfter + ",\"lowSourcePosition\":" + lowSpBase64 + ",\"highSourcePosition\":" + highSpString + " }";
        }
        catch (IOException e) {
            Path.logger.error((Object)e);
            e.printStackTrace();
            return null;
        }
    }
    
    private String getBase64EncodedLowSourcePosition() throws IOException {
        final ByteArrayOutputStream lowToBytes = new ByteArrayOutputStream();
        final ObjectOutputStream lowObjectOutputStream = new ObjectOutputStream(lowToBytes);
        lowObjectOutputStream.writeObject(this.lowSourcePosition);
        final String lowSpBase64 = DatatypeConverter.printHexBinary(lowToBytes.toByteArray());
        return lowSpBase64;
    }
    
    private String getBase64EncodedHighSourcePosition(final boolean dontAddKey) throws IOException {
        final ByteArrayOutputStream highToBytes = new ByteArrayOutputStream();
        final ObjectOutputStream highObjectOutputStream = new ObjectOutputStream(highToBytes);
        String highSpString = "";
        if (this.highSourcePosition != null) {
            highObjectOutputStream.writeObject(this.highSourcePosition);
            final String highSPHexString = DatatypeConverter.printHexBinary(highToBytes.toByteArray());
            if (dontAddKey) {
                highSpString = highSPHexString;
            }
            else {
                highSpString = " highSourcePosition:" + highSPHexString;
            }
        }
        return highSpString;
    }
    
    public static Path fromJson(final String json) {
        try {
            UUID uuid = null;
            ItemList pathItems = null;
            String atOrAfter = null;
            SourcePosition lowSourcePosition = null;
            SourcePosition highSourcePosition = null;
            Pattern r = Pattern.compile("flowUuid:\\s*([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})");
            Matcher m = r.matcher(json);
            if (m.find()) {
                uuid = new UUID(m.group(1));
            }
            if (uuid == null) {
                System.out.println("Could not parse UUID from " + json);
                return null;
            }
            final String regex = "pathItems:\\s*((?:[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}\\+[\\w\\-\\*]+,)*[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}\\+[\\w\\-\\*]+)";
            r = Pattern.compile(regex);
            m = r.matcher(json);
            if (m.find()) {
                pathItems = ItemList.fromString(m.group(1));
            }
            if (pathItems == null) {
                System.out.println("Could not parse pathItems from " + json + " using pattern " + regex);
                return null;
            }
            r = Pattern.compile("atOrAfter:\\s*([@^])");
            m = r.matcher(json);
            if (m.find()) {
                atOrAfter = m.group(1);
            }
            if (atOrAfter == null) {
                atOrAfter = "@";
            }
            final String regex2 = "l?o?w?[sS]ourcePosition:\\s*([\\w\\+/=]*)";
            r = Pattern.compile(regex2);
            m = r.matcher(json);
            if (m.find()) {
                final byte[] spBytes = DatatypeConverter.parseHexBinary(m.group(1));
                final ByteArrayInputStream fromBytes = new ByteArrayInputStream(spBytes);
                final ObjectInputStream objectInputStream = new ObjectInputStream(fromBytes);
                lowSourcePosition = (SourcePosition)objectInputStream.readObject();
            }
            if (lowSourcePosition == null) {
                System.out.println("Could not parse lowSourcePosition from " + json);
                return null;
            }
            r = Pattern.compile("highSourcePosition:\\s?(\\w*)");
            m = r.matcher(json);
            if (m.find()) {
                final byte[] spBytes = DatatypeConverter.parseHexBinary(m.group(1));
                final ByteArrayInputStream fromBytes = new ByteArrayInputStream(spBytes);
                final ObjectInputStream objectInputStream = new ObjectInputStream(fromBytes);
                highSourcePosition = (SourcePosition)objectInputStream.readObject();
            }
            final Path result = new Path(pathItems, lowSourcePosition, highSourcePosition, atOrAfter);
            result.applicationUuid = uuid;
            return result;
        }
        catch (IOException | ClassNotFoundException ex2) {
        		ex2.printStackTrace();
            return null;
        }
    }
    
    public static Path fromStandardJSON(final JsonNode pathJsonNode) {
        try {
            final UUID uuid = null;
            ItemList pathItems = null;
            String atOrAfter = null;
            SourcePosition lowSourcePosition = null;
            SourcePosition highSourcePosition = null;
            final JsonNode pathItemsNode = pathJsonNode.findValue("pathItems");
            if (pathItemsNode == null || pathItemsNode.isNull()) {
                System.out.println("Could not parse pathItems from " + pathJsonNode.toString());
                return null;
            }
            pathItems = ItemList.fromString(stripQuotes(pathItemsNode.toString()));
            final JsonNode atOrAfterNode = pathJsonNode.findValue("atOrAfter");
            if (atOrAfterNode == null || atOrAfterNode.isNull()) {
                atOrAfter = "@";
            }
            atOrAfter = stripQuotes(atOrAfterNode.toString());
            final JsonNode lowSourcePositionNode = pathJsonNode.findValue("lowSourcePosition");
            if (lowSourcePositionNode == null || lowSourcePositionNode.isNull()) {
                System.out.println("Could not parse lowSourcePosition from " + pathJsonNode.toString());
                return null;
            }
            final byte[] spBytes = DatatypeConverter.parseHexBinary(stripQuotes(lowSourcePositionNode.toString()));
            final ByteArrayInputStream fromBytes = new ByteArrayInputStream(spBytes);
            final ObjectInputStream objectInputStream = new ObjectInputStream(fromBytes);
            lowSourcePosition = (SourcePosition)objectInputStream.readObject();
            final JsonNode highSourcePositionNode = pathJsonNode.findValue("highSourcePosition");
            if (highSourcePositionNode != null && !highSourcePositionNode.isNull()) {
                final byte[] spBytes2 = DatatypeConverter.parseHexBinary(stripQuotes(highSourcePositionNode.toString()));
                final ByteArrayInputStream fromBytes2 = new ByteArrayInputStream(spBytes2);
                final ObjectInputStream objectInputStream2 = new ObjectInputStream(fromBytes2);
                highSourcePosition = (SourcePosition)objectInputStream2.readObject();
            }
            final Path result = new Path(pathItems, lowSourcePosition, highSourcePosition, atOrAfter);
            return result;
        }
        catch (IOException | ClassNotFoundException ex2) {
        	ex2.printStackTrace();
            return null;
        }
    }
    
    private static String stripQuotes(final String stringToBeStripped) {
        return stringToBeStripped.substring(1, stringToBeStripped.length() - 1);
    }
    
    @Override
    public String toString() {
        final String result = "Path:" + this.pathItems + this.atOrAfter + this.lowSourcePosition + "//" + this.highSourcePosition;
        return result;
    }
    
    @Override
    public boolean equals(final Object object) {
        if (!(object instanceof Path)) {
            return false;
        }
        final Path that = (Path)object;
        if (this.pathItems.size() != that.size()) {
            return false;
        }
        for (int i = 0; i < this.pathItems.size(); ++i) {
            final Item thisItem = this.getPathComponent(i);
            final Item thatItem = that.getPathComponent(i);
            if (!thisItem.equals(thatItem)) {
                return false;
            }
        }
        return this.lowSourcePosition.compareTo(that.lowSourcePosition) == 0;
    }
    
    @Override
    public int hashCode() {
        int result = 37;
        for (final Item item : this.pathItems.list) {
            result = 31 * result + ((item == null) ? 0 : item.hashCode());
        }
        result = 31 * result + ((this.lowSourcePosition == null) ? 0 : this.lowSourcePosition.hashCode());
        return result;
    }
    
    @Override
    public int compareTo(final Path that) {
        if (that == null) {
            return -1;
        }
        return this.toString().compareTo(that.toString());
    }
    
    public Item getFirstPathItem() {
        final Item result = this.getPathComponent(0);
        return result;
    }
    
    public Item getLastPathItem() {
        final Item result = this.getPathComponent(this.size() - 1);
        return result;
    }
    
    public Path segmentEndingWithUuid(final UUID uuid) {
        final ArrayList<Item> resultItems = new ArrayList<Item>();
        for (int i = 0; i < this.pathItems.list.length; ++i) {
            resultItems.add(this.pathItems.list[i]);
            if (this.pathItems.list[i].componentUuid.equals(uuid)) {
                final Item[] a = new Item[resultItems.size()];
                final ItemList resultItemList = ItemList.get(resultItems.toArray(a));
                final Path result = new Path(resultItemList, this.lowSourcePosition, this.highSourcePosition);
                return result;
            }
        }
        return null;
    }
    
    public Path segmentEndingWithUuid(final UUID uuid, final String distId) {
        final Item[] resultItems = new Item[this.pathItems.list.length];
        for (int i = 0; i < this.pathItems.list.length; ++i) {
            resultItems[i] = this.pathItems.list[i];
            if (this.pathItems.list[i].componentUuid.equals(uuid) && this.pathItems.list[i].distributionID.equals(distId)) {
                final ItemList resultItemList = ItemList.get(resultItems);
                final Path result = new Path(resultItemList, this.lowSourcePosition, this.highSourcePosition);
                return result;
            }
        }
        return null;
    }
    
    public String getAtOrAfter() {
        return this.atOrAfter;
    }
    
    public boolean getAtOrAfterBoolean() {
        return "^".equals(this.atOrAfter);
    }
    
    public boolean isAfter() {
        return this.atOrAfter.equals("^");
    }
    
    public boolean startAtSourcePosition() {
        return !this.atOrAfter.equals("^");
    }
    
    public boolean startAfterSourcePosition() {
        return this.atOrAfter.equals("^");
    }
    
    static {
        Path.logger = Logger.getLogger((Class)Path.class);
    }
    
    public static class Item implements Serializable, KryoSerializable
    {
        private static final long serialVersionUID = 2362934934515089472L;
        private static final String NULL_VAL_STRING = "*";
        private static WeakHashMap<String, Item> internPool;
        public static final String PATTERN = "[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}\\+[\\w\\-\\*]+";
        private UUID componentUuid;
        private String distributionID;
        private String asString;
        
        private Item() {
            this(null, null);
        }
        
        private Item(final UUID componentUuid, final String distributionID) {
            this.componentUuid = componentUuid;
            this.distributionID = distributionID;
        }
        
        public static synchronized Item get(final UUID componentUuid, final String distributionID) {
            final String key = componentUuid + distributionID;
            Item result = Item.internPool.get(key);
            if (result == null) {
                result = new Item(componentUuid, distributionID);
                Item.internPool.put(key, result);
            }
            return result;
        }
        
        public UUID getComponentUUID() {
            return this.componentUuid;
        }
        
        public String getDistributionID() {
            return this.distributionID;
        }
        
        public byte[] toBytes() {
            final byte[] uidbytes = this.componentUuid.toBytes();
            final byte[] keybytes = (this.distributionID != null && !this.distributionID.isEmpty()) ? this.distributionID.getBytes() : "*".getBytes();
            final ByteBuffer buffer = ByteBuffer.allocate(uidbytes.length + keybytes.length);
            buffer.put(uidbytes);
            buffer.put(keybytes);
            return buffer.array();
        }
        
        public void read(final Kryo kryo, final Input input) {
            final boolean uuidNotNull = input.readBoolean();
            this.componentUuid = (uuidNotNull ? ((UUID)kryo.readClassAndObject(input)) : null);
            final boolean distIdNotNull = input.readBoolean();
            this.distributionID = (distIdNotNull ? input.readString() : null);
        }
        
        public void write(final Kryo kryo, final Output output) {
            try {
                output.writeBoolean(this.componentUuid != null);
                if (this.componentUuid != null) {
                    kryo.writeClassAndObject(output, (Object)this.componentUuid);
                }
                output.writeBoolean(this.distributionID != null);
                if (this.distributionID != null) {
                    output.writeString(this.distributionID);
                }
            }
            catch (Exception e) {
                Path.logger.error((Object)"Path Item can't write out");
            }
        }
        
        @Override
        public String toString() {
            if (this.asString == null) {
                final String s1 = (this.componentUuid != null) ? this.componentUuid.toString() : "*";
                final String s2 = (this.distributionID != null && !this.distributionID.isEmpty()) ? this.distributionID : "*";
                this.asString = s1 + "+" + s2;
            }
            return this.asString;
        }
        
        public static Item fromString(final String dataString) {
            final String[] parts = dataString.split("\\+");
            final UUID componentUuid = (parts[0].isEmpty() || parts[0].equals("*")) ? null : new UUID(parts[0]);
            final String distIdString = (parts[1].isEmpty() || parts[1].equals("*")) ? null : parts[1];
            final Item result = new Item(componentUuid, distIdString);
            return result;
        }
        
        @Override
        public boolean equals(final Object object) {
            if (!(object instanceof Item)) {
                return false;
            }
            final Item that = (Item)object;
            return this.componentUuid == null != (that.componentUuid != null) && (this.componentUuid == null || this.componentUuid.equals(that.componentUuid)) && this.distributionID == null != (that.distributionID != null) && (this.distributionID == null || this.distributionID.equals(that.distributionID));
        }
        
        @Override
        public int hashCode() {
            int result = 31;
            result = 37 * result + ((this.componentUuid == null) ? 0 : this.componentUuid.hashCode());
            result = 37 * result + ((this.distributionID == null) ? 0 : this.distributionID.hashCode());
            return result;
        }
        
        static {
            Item.internPool = new WeakHashMap<String, Item>();
        }
    }
    
    public static class ItemList implements Serializable
    {
        private static final long serialVersionUID = -7594420778365227945L;
        private static WeakHashMap<String, ItemList> internPool;
        private final Item[] list;
        public static final String PATTERN = "(?:[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}\\+[\\w\\-\\*]+,)*[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}\\+[\\w\\-\\*]+";
        private String asString;
        
        private ItemList() {
            this.list = new Item[0];
        }
        
        private ItemList(final Item[] list) {
            this.list = list;
        }
        
        public static synchronized ItemList get(final Item[] list) {
            final String key = toString(list);
            ItemList result = ItemList.internPool.get(key);
            if (result == null) {
                result = new ItemList(list);
                ItemList.internPool.put(key, result);
            }
            return result;
        }
        
        public static synchronized ItemList get(final Item[] list, final Item item) {
            final String key = toString(list) + ((list.length == 0) ? "" : ",") + item;
            ItemList result = ItemList.internPool.get(key);
            if (result == null) {
                final Item[] augmentedItems = new Item[list.length + 1];
                System.arraycopy(list, 0, augmentedItems, 0, list.length);
                augmentedItems[augmentedItems.length - 1] = item;
                result = new ItemList(augmentedItems);
                ItemList.internPool.put(key, result);
            }
            return result;
        }
        
        public ItemList copyAugmentedWith(final Item item) {
            final Item[] augmentedItems = new Item[this.list.length + 1];
            System.arraycopy(this.list, 0, augmentedItems, 0, this.list.length);
            augmentedItems[augmentedItems.length - 1] = item;
            final ItemList result = new ItemList(augmentedItems);
            return result;
        }
        
        public byte[] getBytes() {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            for (final Item u : this.list) {
                try {
                    os.write(u.toBytes());
                }
                catch (IOException ex) {}
            }
            return os.toByteArray();
        }
        
        public int size() {
            return this.list.length;
        }
        
        public Item get(final int index) {
            return this.list[index];
        }
        
        @Override
        public String toString() {
            if (this.asString == null) {
                this.asString = toString(this.list);
            }
            return this.asString;
        }
        
        private static String toString(final Item[] list) {
            final StringBuilder result = new StringBuilder();
            boolean first = true;
            for (final Item u : list) {
                if (!first) {
                    result.append(",");
                }
                first = false;
                result.append(u);
            }
            return result.toString();
        }
        
        @Override
        public int hashCode() {
            return this.toString().hashCode();
        }
        
        public static ItemList fromString(final String dataValue) {
            final String[] parts = dataValue.split(",");
            final Item[] items = new Item[parts.length];
            for (int i = 0; i < parts.length; ++i) {
                items[i] = Item.fromString(parts[i]);
            }
            final ItemList result = get(items);
            return result;
        }
        
        static {
            ItemList.internPool = new WeakHashMap<String, ItemList>();
        }
    }
}
