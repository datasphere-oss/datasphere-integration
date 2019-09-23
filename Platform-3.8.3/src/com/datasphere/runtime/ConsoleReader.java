package com.datasphere.runtime;

import org.apache.log4j.*;
import java.io.*;
import jline.*;
import java.nio.file.*;

public final class ConsoleReader
{
    private static Logger logger;
    private static final jline.ConsoleReader jlineConsole;
    private static final Console systemConsole;
    protected static final String StdInputAbsentError = "No Standard Input available, hence terminating.";
    
    private static jline.ConsoleReader getJLineConsole() {
        jline.ConsoleReader console = null;
        try {
            console = new jline.ConsoleReader();
            console.setUseHistory(false);
            if (ConsoleReader.logger.isDebugEnabled()) {
                ConsoleReader.logger.debug((Object)"Using JLine console");
            }
        }
        catch (NoClassDefFoundError e) {
            if (ConsoleReader.logger.isDebugEnabled()) {
                ConsoleReader.logger.debug((Object)"JLine console not available. Using system console");
            }
        }
        catch (IOException e2) {
            if (ConsoleReader.logger.isDebugEnabled()) {
                ConsoleReader.logger.debug((Object)"JLine console IO error. Using system console");
            }
        }
        return console;
    }
    
    public static String readLine(final String prompt) throws IOException {
        if (ConsoleReader.jlineConsole != null) {
            return ConsoleReader.jlineConsole.readLine(prompt);
        }
        if (prompt != null && !prompt.isEmpty()) {
            printf(prompt);
        }
        if (ConsoleReader.systemConsole != null) {
            return ConsoleReader.systemConsole.readLine();
        }
        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        return reader.readLine();
    }
    
    public static String readLine() throws IOException {
        return readLine("");
    }
    
    public static String readLineRespond(final String prompt) throws IOException {
        final String read = readLine(prompt);
        if (read == null) {
            System.err.println("No Standard Input available, hence terminating.");
            System.exit(0);
        }
        return read;
    }
    
    public static String readPassword(final String prompt) throws IOException {
        if (ConsoleReader.jlineConsole != null) {
            return ConsoleReader.jlineConsole.readLine(prompt, new Character('*'));
        }
        printf(prompt);
        if (ConsoleReader.systemConsole != null) {
            return new String(System.console().readPassword());
        }
        final InputStream inputReader = System.in;
        BufferedReader reader = null;
        if (inputReader != null) {
            reader = new BufferedReader(new InputStreamReader(System.in));
        }
        else {
            System.err.println("No Standard Input available, hence terminating.");
            System.exit(0);
        }
        return reader.readLine();
    }
    
    public static void enableHistory() {
        if (ConsoleReader.jlineConsole != null) {
            final File historyFile = new File(System.getProperty("user.home"), ".hd_history");
            final Path historyPath = historyFile.toPath();
            Boolean hideFile = true;
            if (ConsoleReader.logger.isDebugEnabled()) {
                ConsoleReader.logger.debug((Object)("HD history file is " + historyFile.toString()));
            }
            try {
                Files.setAttribute(historyPath, "dos:hidden", Boolean.FALSE, LinkOption.NOFOLLOW_LINKS);
                if (ConsoleReader.logger.isDebugEnabled()) {
                    ConsoleReader.logger.debug((Object)"Cleared DOS Hidden attribute successfully");
                }
            }
            catch (NoSuchFileException e2) {
                hideFile = true;
            }
            catch (UnsupportedOperationException | IOException ex3) {
                hideFile = false;
                if (ConsoleReader.logger.isDebugEnabled()) {
                    ConsoleReader.logger.debug((Object)("Failed to unhide history file '" + historyFile + "', " + ex3));
                }
            }
            try {
                ConsoleReader.jlineConsole.setHistory(new History(historyFile));
                if (ConsoleReader.logger.isDebugEnabled()) {
                    ConsoleReader.logger.debug((Object)"Set history file successfully");
                }
                if (hideFile) {
                    Files.setAttribute(historyPath, "dos:hidden", Boolean.TRUE, LinkOption.NOFOLLOW_LINKS);
                    if (ConsoleReader.logger.isDebugEnabled()) {
                        ConsoleReader.logger.debug((Object)"Set DOS Hidden attribute successfully");
                    }
                }
            }
            catch (UnsupportedOperationException | IOException ex4) {
                if (ConsoleReader.logger.isDebugEnabled()) {
                    ConsoleReader.logger.debug((Object)("Problem setting up history file, '" + historyFile + "', " + ex4));
                }
            }
            ConsoleReader.jlineConsole.setUseHistory(true);
        }
    }
    
    public static void enableHistoryWithoutFile() {
        if (ConsoleReader.jlineConsole != null) {
            ConsoleReader.jlineConsole.setHistory(new History());
            if (ConsoleReader.logger.isDebugEnabled()) {
                ConsoleReader.logger.debug((Object)"History is set successfully without file.");
            }
            ConsoleReader.jlineConsole.setUseHistory(true);
        }
    }
    
    public static void clearHistory() {
        ConsoleReader.jlineConsole.setHistory(new History());
    }
    
    public static void disableHistory() {
        if (ConsoleReader.jlineConsole != null) {
            ConsoleReader.jlineConsole.setUseHistory(false);
        }
    }
    
    public static void printf(final String toPrint) {
        if (ConsoleReader.systemConsole != null) {
            ConsoleReader.systemConsole.printf(toPrint, new Object[0]);
        }
        else {
            synchronized (System.out) {
                System.out.print(toPrint);
                System.out.flush();
            }
        }
    }
    
    static {
        ConsoleReader.logger = Logger.getLogger((Class)ConsoleReader.class);
        jlineConsole = getJLineConsole();
        systemConsole = System.console();
    }
}
