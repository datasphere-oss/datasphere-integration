package com.datasphere.tungsten;

import org.apache.log4j.*;
import org.eclipse.jetty.websocket.*;
import java.util.concurrent.*;
import java.io.*;
import java.util.*;
import java.net.*;

public class SimpleWebSocketClient
{
    private static Logger logger;
    String url;
    WebSocketClient client;
    ClientSideSocket socket;
    WebSocket.Connection connection;
    WebSocketClientFactory factory;
    
    public SimpleWebSocketClient(final String url) {
        this.client = null;
        this.socket = null;
        this.factory = new WebSocketClientFactory();
        this.url = url;
    }
    
    public void start() {
        URI destinationUri = null;
        try {
            if (this.url != null) {
                destinationUri = new URI(this.url);
            }
            if (!this.factory.isStarted()) {
                this.factory.start();
            }
            this.client = this.factory.newWebSocketClient();
            this.socket = new ClientSideSocket();
            final Future<WebSocket.Connection> future = (Future<WebSocket.Connection>)this.client.open(destinationUri, (WebSocket)this.socket);
            while (future == null) {
                Thread.sleep(10L);
            }
            this.connection = future.get();
        }
        catch (Throwable t) {
            if (SimpleWebSocketClient.logger.isDebugEnabled()) {
                SimpleWebSocketClient.logger.debug((Object)("error making a socket connection to url :" + this.url));
            }
        }
    }
    
    public boolean isConnected() {
        return this.connection != null;
    }
    
    public void sendMessages(final String msg) {
        if (this.connection == null || !this.connection.isOpen()) {
            this.start();
        }
        try {
            this.connection.sendMessage(msg);
        }
        catch (IOException e) {
            if (SimpleWebSocketClient.logger.isDebugEnabled()) {
                SimpleWebSocketClient.logger.debug((Object)"error sending message", (Throwable)e);
            }
        }
    }
    
    public void close() {
        if (this.connection != null && !this.connection.isOpen()) {
            this.connection.close();
        }
        this.connection = null;
    }
    
    public static void main(final String[] args) throws IOException, URISyntaxException, InterruptedException {
        final String url = "ws://localhost:9080/rmiws/";
        final String json = "{\"class\":\"com.datasphere.runtime.QueryValidator\",\"method\":\"compileText\",\"params\":[\"01e4a8b4-ee1f-ab31-8fe0-427e6e16d19b\", \"#command#;\"],\"callbackIndex\":2}";
        final SimpleWebSocketClient tungsteClient = new SimpleWebSocketClient(url);
        tungsteClient.start();
        System.out.println("sending 1st message to remove end point");
        tungsteClient.sendMessages(json);
        Thread.currentThread();
        Thread.sleep(1000L);
        final Scanner scan = new Scanner(System.in);
        String cmd = "help;";
        while (!"quit".equalsIgnoreCase(cmd)) {
            cmd = scan.nextLine();
            json.replace("#command#", cmd);
            tungsteClient.sendMessages(cmd);
        }
        System.out.println("... done");
        tungsteClient.close();
    }
    
    static {
        SimpleWebSocketClient.logger = Logger.getLogger((Class)SimpleWebSocketClient.class);
    }
    
    public static class ClientSideSocket implements WebSocket.OnTextMessage
    {
        public void onOpen(final WebSocket.Connection connection) {
            if (SimpleWebSocketClient.logger.isDebugEnabled()) {
                SimpleWebSocketClient.logger.debug((Object)"socket connection opened");
            }
        }
        
        public void onClose(final int closeCode, final String message) {
            if (SimpleWebSocketClient.logger.isDebugEnabled()) {
                SimpleWebSocketClient.logger.debug((Object)"socket connection closed");
            }
        }
        
        public void onMessage(final String data) {
            if (SimpleWebSocketClient.logger.isDebugEnabled()) {
                SimpleWebSocketClient.logger.debug((Object)("received data from server socket :" + data));
            }
        }
    }
}
