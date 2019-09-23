package java_cup;

import java.io.*;
import java_cup.runtime.*;
import java_cup.runtime.Scanner;

import java.util.*;

public class ProduceDoc
{
    protected static boolean opt_dump_grammar;
    protected static BufferedInputStream input_file;
    protected static PrintWriter parser_class_file;
    protected static PrintWriter symbol_class_file;
    protected static File dest_dir;
    protected static lalr_state start_state;
    protected static parse_action_table action_table;
    protected static parse_reduce_table reduce_table;
    static Map<String, List<production>> productionMap;
    static List<String> terminalList;
    static Map<String, String> lookupMap;
    static String[][] lookups;
    
    public static void main(final String[] argv) throws internal_error, IOException, Exception {
        final boolean did_output = false;
        terminal.clear();
        production.clear();
        action_production.clear();
        emit.clear();
        non_terminal.clear();
        parse_reduce_row.clear();
        parse_action_row.clear();
        lalr_state.clear();
        final File f = new File("./src/main/java/com/hd/runtime/compiler/Grammar.cup");
        if (!f.exists()) {
            System.err.println("input file " + f.getCanonicalPath() + " does not exist");
            System.exit(99);
        }
        final FileInputStream in = new FileInputStream(f);
        System.setIn(in);
        ProduceDoc.input_file = new BufferedInputStream(in);
        parse_grammar_spec();
        if (ErrorManager.getManager().getErrorCount() == 0) {
            check_unused();
            build_parser();
        }
        dump_grammar();
        if (ErrorManager.getManager().getErrorCount() != 0) {
            System.exit(100);
        }
        close_files();
    }
    
    protected static void close_files() throws IOException {
        if (ProduceDoc.input_file != null) {
            ProduceDoc.input_file.close();
        }
    }
    
    protected static void parse_grammar_spec() throws Exception {
        final ComplexSymbolFactory csf = new ComplexSymbolFactory();
        final parser parser_obj = new parser((Scanner)new Lexer(csf), (SymbolFactory)csf);
        try {
            parser_obj.parse();
        }
        catch (Exception e) {
            ErrorManager.getManager().emit_error("Internal error: Unexpected exception");
            throw e;
        }
    }
    
    protected static void check_unused() {
        final Enumeration<?> t = (Enumeration<?>)terminal.all();
        while (t.hasMoreElements()) {
            final terminal term = (terminal)t.nextElement();
            if (term == terminal.EOF) {
                continue;
            }
            if (term == terminal.error) {
                continue;
            }
            if (term.use_count() != 0) {
                continue;
            }
            ++emit.unused_term;
        }
        final Enumeration<?> n = (Enumeration<?>)non_terminal.all();
        while (n.hasMoreElements()) {
            final non_terminal nt = (non_terminal)n.nextElement();
            if (nt.use_count() == 0) {
                ++emit.unused_term;
            }
        }
    }
    
    protected static void build_parser() throws internal_error {
        non_terminal.compute_nullability();
        non_terminal.compute_first_sets();
        ProduceDoc.start_state = lalr_state.build_machine(emit.start_production);
        ProduceDoc.action_table = new parse_action_table();
        ProduceDoc.reduce_table = new parse_reduce_table();
        final Enumeration<?> st = (Enumeration<?>)lalr_state.all();
        while (st.hasMoreElements()) {
            final lalr_state lst = (lalr_state)st.nextElement();
            lst.build_table_entries(ProduceDoc.action_table, ProduceDoc.reduce_table);
        }
        ProduceDoc.action_table.check_reductions();
    }
    
    protected static String plural(final int val) {
        if (val == 1) {
            return "";
        }
        return "s";
    }
    
    protected static String timestr(long time_val, final long total_time) {
        long ms = 0L;
        long sec = 0L;
        final boolean neg = time_val < 0L;
        if (neg) {
            time_val = -time_val;
        }
        ms = time_val % 1000L;
        sec = time_val / 1000L;
        String pad;
        if (sec < 10L) {
            pad = "   ";
        }
        else if (sec < 100L) {
            pad = "  ";
        }
        else if (sec < 1000L) {
            pad = " ";
        }
        else {
            pad = "";
        }
        final long percent10 = time_val * 1000L / total_time;
        return (neg ? "-" : "") + pad + sec + "." + ms % 1000L / 100L + ms % 100L / 10L + ms % 10L + "sec (" + percent10 / 10L + "." + percent10 % 10L + "%)";
    }
    
    public static void dump_grammar() throws internal_error {
        for (final String[] l : ProduceDoc.lookups) {
            ProduceDoc.lookupMap.put(l[0], l[1]);
        }
        for (int tidx = 0; tidx < terminal.number(); ++tidx) {
            ProduceDoc.terminalList.add(terminal.find(tidx).name());
        }
        for (int pidx = 0; pidx < production.number(); ++pidx) {
            final production prod = production.find(pidx);
            final String name = prod.lhs().the_symbol().name();
            List<production> ps = ProduceDoc.productionMap.get(name);
            if (ps == null) {
                ps = new ArrayList<production>();
                ProduceDoc.productionMap.put(name, ps);
            }
            ps.add(prod);
        }
        final Set<String> done = new HashSet<String>();
        System.out.println("<html>\n  <body>");
        outputRecursively("stmt", done);
        System.out.println("  </body>\n</html>");
    }
    
    public static boolean empty(final production p) throws internal_error {
        boolean empty = true;
        for (int i = 0; i < p.rhs_length(); ++i) {
            final String name = ((symbol_part)p.rhs(i)).the_symbol().name();
            if (name.trim().length() > 0) {
                empty = false;
                break;
            }
        }
        return empty;
    }
    
    public static void outputRecursively(final String what, final Set<String> done) throws internal_error {
        if (!done.contains(what)) {
            System.out.println("    <a id=" + what + "><h3>" + what + "</h3></a>");
            final List<production> ps = ProduceDoc.productionMap.get(what);
            done.add(what);
            System.out.println("    <ol>");
            for (final production p : ps) {
                System.out.print("      <li>");
                if (empty(p)) {
                    System.out.print(" <BLANK> ");
                }
                else {
                    for (int i = 0; i < p.rhs_length(); ++i) {
                        final String name = ((symbol_part)p.rhs(i)).the_symbol().name();
                        final boolean terminal = ProduceDoc.terminalList.contains(name);
                        if (i > 0) {
                            System.out.print(" ");
                        }
                        final String val = ProduceDoc.lookupMap.get(name);
                        if (val != null) {
                            System.out.print(val);
                        }
                        else if (terminal) {
                            System.out.print(name);
                        }
                        else {
                            System.out.print("<a href='#" + name + "'>" + name + "</a>");
                        }
                    }
                }
                System.out.println("</li>");
            }
            System.out.println("    </ol>");
            for (final production p : ps) {
                if (empty(p)) {
                    continue;
                }
                for (int i = 0; i < p.rhs_length(); ++i) {
                    final String name = ((symbol_part)p.rhs(i)).the_symbol().name();
                    final boolean terminal = ProduceDoc.terminalList.contains(name);
                    if (!terminal) {
                        outputRecursively(name, done);
                    }
                }
            }
        }
    }
    
    static {
        ProduceDoc.opt_dump_grammar = true;
        ProduceDoc.dest_dir = null;
        ProduceDoc.productionMap = new HashMap<String, List<production>>();
        ProduceDoc.terminalList = new ArrayList<String>();
        ProduceDoc.lookupMap = new HashMap<String, String>();
        ProduceDoc.lookups = new String[][] { { "$EQ", "=" }, { "$NOTEQ", "!=" }, { "$LT", "&lt;" }, { "$GT", "&gt;" }, { "$EQEQ", "==" }, { "$LTEQ", "&lt;=" }, { "$GTEQ", "&gt;=" }, { "$LTGT", "&lt;&gt;" }, { "$LSHIFT", "&lt;&lt;" }, { "$RSHIFT", "&gt;&gt;" }, { "$URSHIFT", "&gt;U&gt;" }, { "$BITOR", "|" }, { "$BITXOR", "~" }, { "$BITAND", "&amp;" }, { "$UNARY", "$" }, { "$PLUS", "+" }, { "$MINUS", "-" }, { "$MULT", "*" }, { "$DIV", "/" }, { "$MOD", "%" }, { "$TILDE", "~" }, { "$COMMA", "," }, { "$DOT", "." }, { "$SEMICOLON", ";" }, { "$COLON", ":" }, { "$LBRACE", "{" }, { "$RBRACE", "}" }, { "$LBRACK", "[" }, { "$RBRACK", "]" }, { "$LPAREN", "(" }, { "$RPAREN", ")" }, { "$SQ_STRING_LITERAL", "'{string}'" }, { "$DQ_STRING_LITERAL", "\"{string}\"" }, { "$IDENTIFIER", "{ident}" }, { "$INTEGER_LITERAL", "{int}" }, { "$LONG_LITERAL", "{long}" }, { "$FLOAT_LITERAL", "{float}" }, { "$DOUBLE_LITERAL", "{double}" }, { "ident", "{ident}" }, { "name", "{name}" } };
    }
}
