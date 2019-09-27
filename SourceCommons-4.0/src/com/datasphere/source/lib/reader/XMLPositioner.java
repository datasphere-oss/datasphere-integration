package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderMalfunctionError;
import java.nio.charset.CoderResult;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.prop.Property;

public class XMLPositioner extends ReaderBase
{
    ByteBuffer buffer;
    CharBuffer internalCharbuffer;
    String rootNode;
    int totalCharSkipped;
    long bytesSkipped;
    boolean hasPositioned;
    CharsetDecoder decoder;
    String startTag;
    String charSet;
    String sourceName;
    long startPosition;
    private Logger logger;
    
    public XMLPositioner(final ReaderBase link) throws AdapterException {
        super(link);
        this.decoder = null;
        this.startTag = null;
        this.charSet = null;
        this.sourceName = "";
        this.startPosition = 0L;
        this.logger = Logger.getLogger((Class)XMLPositioner.class);
        final Property property = this.property();
        this.property().getClass();
        this.rootNode = property.getString("rootnode", "/");
        this.totalCharSkipped = 0;
        this.init(this.hasPositioned = false);
    }
    
    @Override
    public long skip(final long position) {
        try {
            this.buffer = this.positionTheReader(position, true, false);
        }
        catch (AdapterException e) {
            e.printStackTrace();
        }
        return position;
    }
    
    public long startPosition() {
        return this.startPosition;
    }
    
    public long beginOffset() {
        return this.totalCharSkipped;
    }
    
    @Override
    public CheckpointDetail getCheckpointDetail() {
        return this.recoveryCheckpoint;
    }
    
    @Override
    public void position(final CheckpointDetail checkpoint, final boolean positionFlag) throws AdapterException {
        this.linkedStrategy.position(checkpoint, false);
        if (checkpoint == null || checkpoint.getRecordEndOffset() == null) {
            if (checkpoint != null || this.linkedStrategy.property().positionByEOF) {
                long bytesToSkip;
                if (checkpoint != null) {
                    bytesToSkip = checkpoint.seekPosition();
                }
                else {
                    bytesToSkip = this.linkedStrategy.getEOFPosition();
                }
                this.positionTheReader(0L, false, true);
                this.skipBytes(bytesToSkip);
                this.startPosition = bytesToSkip;
                this.recoveryCheckpoint.seekPosition(bytesToSkip);
                this.buffer = this.positionTheReader(0L, true, false);
            }
            else if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)"XMLPositioner::position() : Starting from BEGIN posiotion");
            }
            return;
        }
        this.startPosition = checkpoint.seekPosition();
        this.positionTheReader(0L, false, true);
        this.skipBytes(checkpoint.seekPosition());
        this.recoveryCheckpoint.seekPosition((long)checkpoint.seekPosition());
        this.buffer = this.positionTheReader(checkpoint.getRecordEndOffset(), true, false);
        if (this.buffer != null) {
            this.recoveryCheckpoint.setSourceName(checkpoint.getSourceName());
            this.recoveryCheckpoint.setRecordBeginOffset((long)this.recoveryCheckpoint.getRecordBeginOffset());
            this.recoveryCheckpoint.setRecordEndOffset(0L);
        }
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        if (!this.hasPositioned) {
            this.buffer = this.positionTheReader(0L, true, false);
            return null;
        }
        if (this.buffer != null && this.buffer.hasRemaining()) {
            return this.buffer;
        }
        return this.linkedStrategy.readBlock();
    }
    
    @Override
    public void close() throws IOException {
        super.close();
    }
    
    private String getCharset(final ByteBuffer buffer) {
        if (buffer != null) {
            final String buf = new String(buffer.array(), Charset.forName("UTF-8"));
            final int offset = buf.indexOf(">");
            if (offset == -1) {
                return "";
            }
            final int charSetOffset = buf.indexOf("encoding");
            if (charSetOffset != -1) {
                final int endOfCharSet = buf.indexOf(32, charSetOffset);
                if (endOfCharSet != -1) {
                    final int beginOfCharSet = buf.lastIndexOf(61, endOfCharSet);
                    if (beginOfCharSet != -1) {
                        final String charSet = buf.substring(beginOfCharSet + 1, endOfCharSet);
                        return charSet;
                    }
                }
            }
        }
        return "UTF-8";
    }
    
    private String extractHeader(final ByteBuffer buff) {
        if (buff != null) {
            final String buf = new String(buff.array(), Charset.forName("UTF-8"));
            final int offset = buf.indexOf(">");
            if (offset != -1) {
                String header = "";
                if (buf.indexOf("xml") != -1) {
                    header = buf.substring(0, offset + 1);
                }
                return header;
            }
        }
        return "";
    }
    
    private String formHeader(final String rootNode, final String header, final String charSet) {
        String xmlHeader = header;
        final String[] nodeList = rootNode.split("/");
        for (int idx = 0; idx < nodeList.length - 1; ++idx) {
            if (nodeList[idx].length() != 0) {
                xmlHeader = xmlHeader + "<" + nodeList[idx] + ">\n";
            }
        }
        return xmlHeader;
    }
    
    private ByteBuffer formInitialBuffer(final String header, final CharBuffer leftOutData, final String charSet) throws AdapterException {
        String xmlString = "";
        xmlString += header;
        final Charset charset = Charset.forName(charSet);
        final CharsetEncoder encoder = charset.newEncoder();
        ByteBuffer byteBuff = null;
        xmlString += leftOutData.toString();
        final CharBuffer buffer = CharBuffer.allocate(xmlString.length());
        buffer.put(xmlString);
        buffer.flip();
        try {
            byteBuff = encoder.encode(buffer);
        }
        catch (CharacterCodingException e) {
            throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME, (Throwable)e);
        }
        return byteBuff;
    }
    
    private ByteBuffer positionTheReader(final long charOffset, final boolean isCharOffset, final boolean initalizeHeader) throws AdapterException {
        long charToSkip = charOffset;
        final ByteBuffer tmpBuffer = ByteBuffer.allocate(this.blockSize() * 2);
        final String[] nodeList = this.rootNode.split("/");
        final String startTagOfLeafe = "<" + nodeList[nodeList.length - 1] + ">";
        final String startTagOfLeafeOtherForm = "<" + nodeList[nodeList.length - 1] + " ";
        if (this.sourceName.length() == 0) {
            this.sourceName = this.name();
        }
        while (!this.stopRead && !this.hasPositioned) {
            try {
                final ByteBuffer tmpBuf = (ByteBuffer)this.linkedStrategy.readBlock();
                if (tmpBuf == null) {
                    return null;
                }
                if (!this.sourceName.equals(this.name())) {
                    this.recoveryCheckpoint.setSourceName(this.name());
                    this.recoveryCheckpoint.seekPosition(0L);
                    this.sourceName = this.name();
                }
                tmpBuffer.clear();
                tmpBuffer.flip();
                System.arraycopy(tmpBuf.array(), 0, tmpBuffer.array(), tmpBuffer.limit(), tmpBuf.limit());
                tmpBuffer.limit(tmpBuffer.limit() + tmpBuf.limit());
                if (this.decoder == null) {
                    this.charSet = this.getCharset(tmpBuffer);
                    if (this.charSet.length() == 0 || (!isCharOffset && charToSkip <= tmpBuf.limit() && !initalizeHeader)) {
                        this.bytesSkipped = 0L;
                        this.hasPositioned = true;
                        return tmpBuffer;
                    }
                    this.startTag = this.extractHeader(tmpBuffer);
                    this.decoder = Charset.forName(this.charSet).newDecoder();
                    if (initalizeHeader) {
                        return null;
                    }
                    this.bytesSkipped = charToSkip;
                    this.startPosition = charToSkip;
                }
                final CharBuffer charBuff = CharBuffer.allocate(this.blockSize());
                charBuff.clear();
                final CoderResult result = this.decoder.decode(tmpBuffer, charBuff, false);
                if (result.isError()) {
                    throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME);
                }
                charBuff.flip();
                if (isCharOffset && charToSkip > charBuff.limit()) {
                    charToSkip -= charBuff.limit();
                    this.totalCharSkipped += charBuff.limit();
                    continue;
                }
                if (!isCharOffset && charToSkip > tmpBuffer.limit()) {
                    charToSkip -= tmpBuffer.limit();
                    this.totalCharSkipped += charBuff.limit();
                    continue;
                }
                if (!isCharOffset) {
                    if (this.totalCharSkipped == 0) {
                        charBuff.position(this.totalCharSkipped);
                    }
                }
                else if (charToSkip > 0L) {
                    charBuff.position((int)charToSkip);
                    charToSkip = 0L;
                }
                else {
                    charBuff.position((int)charToSkip);
                }
                final String tmpStr = charBuff.toString();
                int startTagOff = tmpStr.indexOf(startTagOfLeafe);
                if (startTagOff == -1) {
                    startTagOff = tmpStr.indexOf(startTagOfLeafeOtherForm);
                }
                if (startTagOff != -1) {
                    final int tmp = startTagOff;
                    charBuff.position(charBuff.position() + tmp);
                    final String xmlHeader = this.formHeader(this.rootNode, this.startTag, this.charSet);
                    if (charToSkip == 0L) {
                        this.totalCharSkipped += charBuff.position();
                        this.recoveryCheckpoint.setRecordBeginOffset((long)this.totalCharSkipped);
                    }
                    this.totalCharSkipped = xmlHeader.length();
                    this.recoveryCheckpoint.setRecordLength((long)this.totalCharSkipped);
                    this.hasPositioned = true;
                    final ByteBuffer x = this.formInitialBuffer(xmlHeader, charBuff, this.charSet);
                    return x;
                }
                this.totalCharSkipped += charBuff.limit();
                tmpBuffer.clear();
                if (charToSkip == 0L) {
                    return null;
                }
                continue;
            }
            catch (CoderMalfunctionError exp) {
                throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME);
            }
        }
        return null;
    }
}
