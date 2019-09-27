package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderMalfunctionError;
import java.nio.charset.CoderResult;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.prop.Property;

public class Encoding extends ReaderBase
{
    private final String DEFAULT_ID = "Default";
    private CharsetDecoder decoder;
    private ByteBuffer byteBuffer;
    CoderResult decodeLoop;
    CharBuffer buffer;
    private boolean recovery;
    Logger logger;
    CharBuffer charBuff;
    boolean leftOutFlag;
    ByteBuffer leftOut;
    private char characterToReplace;
    
    public Encoding(final ReaderBase link) throws AdapterException {
        super(link);
        this.recovery = false;
        this.logger = Logger.getLogger((Class)Encoding.class);
        this.characterToReplace = '?';
    }
    
    public Encoding(final Property prop) throws AdapterException {
        super(prop);
        this.recovery = false;
        this.logger = Logger.getLogger((Class)Encoding.class);
        this.characterToReplace = '?';
    }
    
    public void init() throws AdapterException {
        super.init();
        this.buffer = null;
        final String charSet = this.CharSet();
        if (charSet != null) {
            this.decoder = Charset.forName(charSet).newDecoder();
        }
        else {
            this.decoder = Charset.forName("UTF-8").newDecoder();
        }
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("Encoding layer has been intialized with charset " + this.decoder.charset().name() + ". System's default charset is " + Charset.defaultCharset().name()));
        }
        this.charBuff = CharBuffer.allocate((int)(this.blockSize() * 1.5));
        this.leftOut = ByteBuffer.allocate(this.blockSize());
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        if (this.buffer != null && this.recovery) {
            this.recovery = false;
            return this.buffer;
        }
        this.byteBuffer = (ByteBuffer)this.linkedStrategy.readBlock();
        if (this.byteBuffer == null) {
            return null;
        }
        if (this.leftOutFlag) {
            this.leftOutFlag = false;
            final int limit = this.leftOut.limit();
            if (this.logger.isDebugEnabled()) {
                this.logger.debug((Object)("Going to copy left out binary data of size {" + limit + "}"));
            }
            final int size = this.byteBuffer.limit() + limit;
            final int additionalBytesRequired = limit - (this.byteBuffer.capacity() - this.byteBuffer.limit());
            if (additionalBytesRequired > 0) {
                final ByteBuffer tmp = ByteBuffer.allocate(this.byteBuffer.capacity() + additionalBytesRequired);
                tmp.put(this.leftOut);
                tmp.put(this.byteBuffer);
                tmp.flip();
                this.byteBuffer = tmp;
            }
            else {
                System.arraycopy(this.byteBuffer.array(), 0, this.byteBuffer.array(), limit, this.byteBuffer.limit());
                System.arraycopy(this.leftOut.array(), 0, this.byteBuffer.array(), 0, limit);
                this.byteBuffer.position(0);
                this.byteBuffer.limit(size);
            }
        }
        return this.decode(this.byteBuffer);
    }
    
    @Override
    public Object readBlock(final boolean multiEndpointSupport) throws AdapterException {
        if (this.buffer != null && this.recovery) {
            this.recovery = false;
            return this.buffer;
        }
        Object obj = this.linkedStrategy.readBlock();
        if (obj != null) {
            if (obj instanceof DataPacket) {
                final DataPacket packet = (DataPacket)obj;
                if (packet != null) {
                    packet.data(this.decode((ByteBuffer)packet.data()));
                }
            }
            else {
                obj = new DataPacket(obj, "Default");
            }
        }
        return obj;
    }
    
    private CharBuffer recoveryData(final ByteBuffer buffer, final CharBuffer charBuffer) {
        final CharBuffer tmpCharBuffer = CharBuffer.allocate(this.blockSize());
        tmpCharBuffer.flip();
        int charCount = 0;
        try {
            final int bytesConverted = charBuffer.toString().getBytes(this.CharSet()).length;
            int bytesToProcess = buffer.limit() - bytesConverted;
            final CharBuffer charBuf = CharBuffer.allocate(this.blockSize());
            charBuf.limit(0);
            final ByteBuffer tmpBuf = ByteBuffer.allocate(bytesToProcess);
            System.arraycopy(buffer.array(), bytesConverted, tmpBuf.array(), 0, bytesToProcess);
            tmpBuf.limit(bytesToProcess);
            while (bytesToProcess > 10) {
                tmpBuf.limit(bytesToProcess);
                tmpBuf.position(0);
                charBuf.clear();
                final CoderResult result = this.decoder.decode(tmpBuf, charBuf, false);
                charBuf.flip();
                int bytesDecoded = 0;
                if (!result.isError()) {
                    break;
                }
                if (!result.isMalformed()) {
                    this.logger.warn((Object)"Could not recover data.");
                    return null;
                }
                if (charBuf.limit() > 0) {
                    if (tmpCharBuffer.limit() > 0) {
                        System.arraycopy(tmpCharBuffer.array(), 0, charBuffer.array(), charBuffer.length(), tmpCharBuffer.limit());
                        charBuffer.limit(charBuffer.limit() + tmpCharBuffer.limit());
                        charCount = 0;
                        tmpCharBuffer.clear();
                        tmpCharBuffer.flip();
                    }
                    System.arraycopy(charBuf.array(), 0, charBuffer.array(), charBuffer.limit(), charBuf.limit());
                    charBuffer.limit(charBuffer.limit() + charBuf.limit());
                    bytesDecoded = charBuf.toString().getBytes(this.CharSet()).length;
                    bytesToProcess -= charBuf.toString().getBytes(this.CharSet()).length;
                    charBuf.limit(0);
                }
                else {
                    for (int idx = 0; idx < result.length(); ++idx) {
                        tmpCharBuffer.limit(tmpCharBuffer.limit() + 1);
                        tmpCharBuffer.append(this.characterToReplace);
                    }
                    charCount += result.length();
                    bytesToProcess -= result.length();
                    bytesDecoded = result.length();
                }
                System.arraycopy(tmpBuf.array(), bytesDecoded, tmpBuf.array(), 0, bytesToProcess);
                tmpBuf.limit(bytesToProcess);
            }
            if (bytesToProcess < 10) {
                System.arraycopy(tmpBuf.array(), 0, this.leftOut.array(), 0, tmpBuf.limit());
                this.leftOut.limit(tmpBuf.limit());
                this.leftOutFlag = true;
            }
            if (tmpCharBuffer.limit() > 0) {
                System.arraycopy(tmpCharBuffer.array(), 0, charBuffer.array(), charBuffer.limit(), tmpCharBuffer.limit());
                charBuffer.limit(charBuffer.limit() + tmpCharBuffer.limit());
            }
            if (charBuf.limit() > 0) {
                System.arraycopy(charBuf.array(), 0, charBuffer.array(), charBuffer.limit(), charBuf.limit());
                charBuffer.limit(charBuffer.limit() + charBuf.limit());
            }
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return charBuffer;
    }
    
    private synchronized CharBuffer decode(final ByteBuffer buffer) throws AdapterException {
        try {
            CoderResult result = null;
            this.charBuff.clear();
            result = this.decoder.decode(buffer, this.charBuff, false);
            this.charBuff.flip();
            if (result.isError()) {
                if (result.isMalformed()) {
                    this.logger.warn((Object)"Decoding failed, will try to recovery.");
                    final int bytesLost = result.length();
                    final int bytesConverted = this.charBuff.toString().getBytes(this.CharSet()).length;
                    this.charBuff = this.recoveryData(buffer, this.charBuff);
                    if (this.charBuff != null) {
                        return this.charBuff;
                    }
                }
                this.logger.warn((Object)("Error while decoding the data (charset used: " + this.CharSet() + ")"));
                throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME);
            }
            return this.charBuff;
        }
        catch (CoderMalfunctionError | UnsupportedEncodingException coderMalfunctionError) {
            this.logger.warn((Object)("Error while decoding the data : " + coderMalfunctionError.getMessage()));
            throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME);
        }
    }
    
    @Override
    public void close() throws IOException {
        this.linkedStrategy.close();
        this.byteBuffer = null;
        this.charBuff.clear();
        this.leftOut.clear();
        this.charBuff = null;
    }
    
    @Override
    public int available() {
        final int len = this.buffer.limit() - this.buffer.position();
        return len;
    }
    
    @Override
    public int read() throws IOException {
        if (this.buffer.position() < this.buffer.limit()) {
            return this.buffer.get();
        }
        try {
            this.buffer = (CharBuffer)this.readBlock();
            if (this.buffer != null) {
                return this.buffer.get();
            }
            return -1;
        }
        catch (AdapterException exp) {
            exp.printStackTrace();
            return 0;
        }
    }
    
    @Override
    public void position(final CheckpointDetail attr, final boolean pos) throws AdapterException {
        this.linkedStrategy.position(attr, pos);
        if (attr != null && attr.getRecordBeginOffset() != null) {
            this.skip(attr.getRecordBeginOffset());
        }
    }
    
    @Override
    public void setCheckPointDetails(final CheckpointDetail cp) {
        if (this.linkedStrategy != null) {
            this.linkedStrategy.setCheckPointDetails(cp);
        }
        else {
            this.recoveryCheckpoint = cp;
        }
    }
    
    @Override
    public long skip(long characterOffset) {
        this.recovery = true;
        int charactersInBuffer = 0;
        final long charactersToBeSkipped = characterOffset;
        while (characterOffset > 0L) {
            try {
                final CharBuffer charbuff = (CharBuffer)this.readBlock();
                if (charbuff == null) {
                    if (charactersInBuffer > 0) {
                        this.logger.error((Object)("Characters to be skipped is [" + charactersToBeSkipped + "] but characters skipped is [" + characterOffset + "]"));
                    }
                    else {
                        this.logger.error((Object)("Characters to be skipped is [" + charactersToBeSkipped + "] but characters skipped is [" + charactersInBuffer + "]"));
                    }
                    break;
                }
                charactersInBuffer = charbuff.length();
                if (characterOffset <= charactersInBuffer) {
                    charactersInBuffer -= (int)characterOffset;
                    if (charactersInBuffer > 0) {
                        final int leftOverToBeCopied = charbuff.length() - (int)characterOffset;
                        this.buffer = CharBuffer.allocate(this.blockSize());
                        System.arraycopy(charbuff.array(), (int)characterOffset, this.buffer.array(), 0, leftOverToBeCopied);
                        this.buffer.limit(leftOverToBeCopied);
                    }
                    break;
                }
                characterOffset -= charactersInBuffer;
            }
            catch (AdapterException e) {
                e.printStackTrace();
            }
        }
        return characterOffset;
    }
}
