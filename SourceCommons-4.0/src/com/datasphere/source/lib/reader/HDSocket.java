package com.datasphere.source.lib.reader;

import java.net.*;
import com.datasphere.common.exc.*;
import com.datasphere.source.lib.prop.*;

import java.io.*;

public class HDSocket extends ReaderBase
{
    protected Socket clientSocket;
    protected String serverIP;
    protected int serverPort;
    protected int listenPort;
    
    public HDSocket(final ReaderBase link) throws AdapterException {
        super(link);
    }
    
    protected HDSocket(final Property prop) throws AdapterException {
        super(prop);
    }
    
    @Override
    protected void init() throws AdapterException {
        super.init();
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        return null;
    }
    
    @Override
    public void close() throws IOException {
    }
    
    public void connect() throws AdapterException {
    }
}
