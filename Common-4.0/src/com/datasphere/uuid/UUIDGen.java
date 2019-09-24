package com.datasphere.uuid;

import java.util.concurrent.atomic.*;
import java.io.*;
import java.net.*;
import java.util.*;
/*
 * 通过系统时钟生成 UUID
 */
public final class UUIDGen
{
    private static String macAddress;
    private static long clockSeqAndNode;
    private static AtomicLong atomicLastTime;
    
    public static long getClockSeqAndNode() {
        return UUIDGen.clockSeqAndNode;
    }
    
    public static long newTime() {
        return createTime(System.currentTimeMillis());
    }
    
    public static long createTime(final long currentTimeMillis) {
        final long timeMillis = currentTimeMillis * 10000L + 122192928000000000L;
        final long currNewLastTime = UUIDGen.atomicLastTime.get();
        if (timeMillis > currNewLastTime) {
            UUIDGen.atomicLastTime.compareAndSet(currNewLastTime, timeMillis);
        }
        return UUIDGen.atomicLastTime.incrementAndGet();
    }
    
    public static String getMACAddress() {
        return UUIDGen.macAddress;
    }
    
    static String getFirstLineOfCommand(final String... commands) throws IOException {
        Process p = null;
        BufferedReader reader = null;
        try {
            p = Runtime.getRuntime().exec(commands);
            reader = new BufferedReader(new InputStreamReader(p.getInputStream()), 128);
            return reader.readLine();
        }
        finally {
            if (p != null) {
                if (reader != null) {
                    try {
                        reader.close();
                    }
                    catch (IOException ex) {}
                }
                try {
                    p.getErrorStream().close();
                }
                catch (IOException ex2) {}
                try {
                    p.getOutputStream().close();
                }
                catch (IOException ex3) {}
                p.destroy();
            }
        }
    }
    
    public static void main(final String[] args) {
        final int numCalcs = 1000000;
        final long bytes = 0L;
        final int numThreads = 4;
        final Thread[] t = new Thread[numThreads];
        final long stime = System.currentTimeMillis();
        for (int i = 0; i < numThreads; ++i) {
            (t[i] = new UUIDGenThread(numCalcs)).start();
        }
        for (int i = 0; i < numThreads; ++i) {
            try {
                t[i].join();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        final long etime = System.currentTimeMillis();
        long diff = etime - stime;
        if (diff == 0L) {
            diff = 1L;
        }
        final double rate = numCalcs * 1000.0 / diff;
        final double brate = bytes * 1000.0 / diff;
        System.out.println("KS: " + numCalcs + " ops " + bytes + " bytes in " + diff + " ms - rate = " + rate + " ops/s " + brate + " bytes/s");
    }
    
    static {
        UUIDGen.macAddress = null;
        UUIDGen.clockSeqAndNode = Long.MIN_VALUE;
        try {
            Class.forName("java.net.InterfaceAddress");
            UUIDGen.macAddress = Class.forName("com.datasphere.uuid.UUIDGen$HardwareAddressLookup").newInstance().toString();
        }
        catch (ExceptionInInitializerError exceptionInInitializerError) {}
        catch (ClassNotFoundException ex2) {}
        catch (LinkageError linkageError) {}
        catch (IllegalAccessException ex3) {}
        catch (InstantiationException ex4) {}
        catch (SecurityException ex5) {}
        if (UUIDGen.macAddress == null) {
            Process p = null;
            BufferedReader in = null;
            try {
                final String osname = System.getProperty("os.name", "");
                final String osver = System.getProperty("os.version", "");
                if (osname.startsWith("Windows")) {
                    p = Runtime.getRuntime().exec(new String[] { "ipconfig", "/all" }, null);
                }
                else if (osname.startsWith("Solaris") || osname.startsWith("SunOS")) {
                    if (osver.startsWith("5.11")) {
                        p = Runtime.getRuntime().exec(new String[] { "dladm", "show-phys", "-m" }, null);
                    }
                    else {
                        final String hostName = getFirstLineOfCommand("uname", "-n");
                        if (hostName != null) {
                            p = Runtime.getRuntime().exec(new String[] { "/usr/sbin/arp", hostName }, null);
                        }
                    }
                }
                else if (new File("/usr/sbin/lanscan").exists()) {
                    p = Runtime.getRuntime().exec(new String[] { "/usr/sbin/lanscan" }, null);
                }
                else if (new File("/sbin/ifconfig").exists()) {
                    p = Runtime.getRuntime().exec(new String[] { "/sbin/ifconfig", "-a" }, null);
                }
                if (p != null) {
                    in = new BufferedReader(new InputStreamReader(p.getInputStream()), 128);
                    String l = null;
                    while ((l = in.readLine()) != null) {
                        UUIDGen.macAddress = MACAddressParser.parse(l);
                        if (UUIDGen.macAddress != null && Hex.parseShort(UUIDGen.macAddress) != 255) {
                            break;
                        }
                    }
                }
            }
            catch (SecurityException ex6) {}
            catch (IOException ex7) {}
            finally {
                if (p != null) {
                    if (in != null) {
                        try {
                            in.close();
                        }
                        catch (IOException ex8) {}
                    }
                    try {
                        p.getErrorStream().close();
                    }
                    catch (IOException ex9) {}
                    try {
                        p.getOutputStream().close();
                    }
                    catch (IOException ex10) {}
                    p.destroy();
                }
            }
        }
        if (UUIDGen.macAddress != null) {
            UUIDGen.clockSeqAndNode |= Hex.parseLong(UUIDGen.macAddress);
        }
        else {
            try {
                final byte[] local = InetAddress.getLocalHost().getAddress();
                UUIDGen.clockSeqAndNode |= (local[0] << 24 & 0xFF000000L);
                UUIDGen.clockSeqAndNode |= (local[1] << 16 & 0xFF0000);
                UUIDGen.clockSeqAndNode |= (local[2] << 8 & 0xFF00);
                UUIDGen.clockSeqAndNode |= (local[3] & 0xFF);
            }
            catch (UnknownHostException ex) {
                UUIDGen.clockSeqAndNode |= (long)(Math.random() * 2.147483647E9);
            }
        }
        UUIDGen.clockSeqAndNode |= (long)(Math.random() * 16383.0) << 48;
        UUIDGen.atomicLastTime = new AtomicLong(Long.MIN_VALUE);
    }
    
    static class HardwareAddressLookup
    {
        @Override
        public String toString() {
            String out = null;
            try {
                final Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
                if (ifs != null) {
                    while (ifs.hasMoreElements()) {
                        final NetworkInterface iface = ifs.nextElement();
                        final byte[] hardware = iface.getHardwareAddress();
                        if (hardware != null && hardware.length == 6 && hardware[1] != -1) {
                            out = Hex.append(new StringBuilder(36), hardware).toString();
                            break;
                        }
                    }
                }
            }
            catch (SocketException ex) {}
            return out;
        }
    }
    
    public static class UUIDGenThread extends Thread
    {
        int numCalcs;
        
        public UUIDGenThread(final int numCalcs) {
            this.numCalcs = 10000000;
            this.numCalcs = numCalcs;
        }
        
        @Override
        public void run() {
            for (int i = 0; i < this.numCalcs; ++i) {
                final UUID uuid = new UUID(System.currentTimeMillis());
                uuid.getTime();
            }
        }
    }
}
