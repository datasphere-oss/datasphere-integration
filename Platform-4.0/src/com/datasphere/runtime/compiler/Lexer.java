package com.datasphere.runtime.compiler;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.datasphere.exception.CompilationException;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.exceptions.UnterminatedTokenException;

import java_cup.runtime.DefaultSymbolFactory;
import java_cup.runtime.Scanner;
import java_cup.runtime.Symbol;
import java_cup.runtime.SymbolFactory;

public class Lexer implements Scanner
{
    private static Logger logger;
    private final Reader reader;
    private StringBuilder buf;
    private Symbol last;
    private FIFO<Symbol> lookahead;
    private int pos;
    private int lastBeginPos;
    private Map<Object, Symbol> index;
    private boolean acceptNewLineInStringLiteral;
    private boolean uiStyleErrors;
    private static Map<String, Integer> keywordsMap;
    
    private Symbol add2index(final Symbol sym) {
        return this.add2index(sym, false);
    }
    
    private Symbol add2indexLeaf(final Symbol sym) {
        return this.add2index(sym, true);
    }
    
    private Symbol add2index(final Symbol sym, final boolean mustbeunique) {
        if (sym.value != null) {
            if (this.index.containsKey(sym.value)) {
                if (mustbeunique) {
                    throw new CompilationException("non-unique object in the stream!!!");
                }
            }
            else {
                this.index.put(sym.value, sym);
            }
        }
        return sym;
    }
    
    public Lexer(final String src, final boolean invokedFromUI) {
        this(new StringReader(src));
        this.uiStyleErrors = invokedFromUI;
    }
    
    public Lexer(final String src) {
        this(new StringReader(src));
        this.uiStyleErrors = false;
    }
    
    public Lexer(final Reader reader) {
        this.buf = new StringBuilder();
        this.last = null;
        this.lookahead = null;
        this.pos = 0;
        this.lastBeginPos = 0;
        this.index = new IdentityHashMap<Object, Symbol>();
        this.acceptNewLineInStringLiteral = true;
        this.uiStyleErrors = false;
        this.reader = reader;
    }
    
    public void setAcceptNewLineInStringLiteral(final boolean yes) {
        this.acceptNewLineInStringLiteral = yes;
    }
    
    public SymbolFactory getSymbolFactory() {
        return (SymbolFactory)new LexerSymbolFactory();
    }
    
    public Pair<Integer, Integer> consumeAll() {
        final Pair<Integer, Integer> ret = Pair.make(this.pos, this.buf.length());
        this.pos = this.buf.length();
        return ret;
    }
    
    private int peek(final int index) throws IOException {
        final int p = this.pos + index;
        int blen = this.buf.length();
        if (p >= blen) {
            int c;
            do {
                c = this.reader.read();
                if (c == -1) {
                    break;
                }
                blen = this.buf.append((char)c).length();
            } while (p >= blen || c != 10);
            if (p >= blen) {
                return -1;
            }
        }
        return this.buf.charAt(p);
    }
    
    private int peek() throws IOException {
        return this.peek(0);
    }
    
    private void consume(final int k) {
        this.pos += k;
        assert this.pos <= this.buf.length();
    }
    
    private void consume() throws IOException {
        this.consume(1);
    }
    
    public Symbol nextToken() throws IOException {
        final Symbol sym = (this.lookahead == null) ? this._nextToken() : this.lookahead.get();
        return this.last = sym;
    }
    
    private Symbol _nextToken() throws IOException {
        final Symbol sym = this.getNextToken();
        sym.left = this.lastBeginPos;
        sym.right = this.pos;
        return sym;
    }
    
    public boolean debug_lex() throws IOException {
        final Symbol sym = this.nextToken();
        System.out.println(sym);
        if (sym.value != null) {
            System.out.println(sym.value.toString());
        }
        return sym.sym != 0;
    }
    
    private Symbol getNextToken() throws IOException {
        while (true) {
            this.lastBeginPos = this.pos;
            final int c = this.peek();
            switch (c) {
                case -1: {
                    return this.newSymbol(0, "EOF");
                }
                case 9:
                case 10:
                case 12:
                case 13:
                case 32: {
                    this.consume();
                    continue;
                }
                case 26: {
                    this.consume();
                    return this.newSymbol(0, "EOF_CTRL_Z");
                }
                case 45: {
                    if (this.peek(1) == 45) {
                        this.consume(2);
                        this.singleLineComment();
                        continue;
                    }
                }
                case 47: {
                    if (this.peek(1) == 42) {
                        this.consume(2);
                        this.consumeMultiLineComment();
                        continue;
                    }
                    return this.getToken(c);
                }
                default: {
                    return this.getToken(c);
                }
            }
        }
    }
    
    private void consumeMultiLineComment() throws IOException {
        while (true) {
            switch (this.peek()) {
                case -1: {
                    throw this.unterminatedTokenError("comment", "/*");
                }
                case 42: {
                    if (this.peek(1) == 47) {
                        this.consume(2);
                        return;
                    }
                    break;
                }
            }
            this.consume();
        }
    }
    
    private void singleLineComment() throws IOException {
    Label_0032:
        while (true) {
            switch (this.peek()) {
                case -1:
                case 10: {
                    break Label_0032;
                }
                default: {
                    this.consume();
                    continue;
                }
            }
        }
    }
    
    private Symbol getToken(final int c) throws IOException {
        switch (c) {
            case 64: {
                this.consume();
                return this.getFileName(64, 228, "string literal", ';');
            }
            case 40: {
                this.consume();
                return this.newSymbol(213, "(");
            }
            case 41: {
                this.consume();
                return this.newSymbol(214, ")");
            }
            case 123: {
                this.consume();
                return this.newSymbol(209, "{");
            }
            case 125: {
                this.consume();
                return this.newSymbol(210, "}");
            }
            case 91: {
                this.consume();
                return this.newSymbol(211, "[");
            }
            case 93: {
                this.consume();
                return this.newSymbol(212, "]");
            }
            case 59: {
                this.consume();
                return this.newSymbol(206, ";");
            }
            case 44: {
                this.consume();
                return this.newSymbol(208, ",");
            }
            case 126: {
                this.consume();
                return this.newSymbol(202, "~");
            }
            case 35: {
                this.consume();
                return this.newSymbol(204, "#");
            }
            case 63: {
                this.consume();
                return this.newSymbol(203, "?");
            }
            case 58: {
                this.consume();
                return this.newSymbol(207, ":");
            }
            case 38: {
                this.consume();
                return this.newSymbol(195, "&");
            }
            case 124: {
                this.consume();
                return this.newSymbol(193, "|");
            }
            case 43: {
                this.consume();
                return this.newSymbol(197, "+");
            }
            case 45: {
                this.consume();
                return this.newSymbol(198, "-");
            }
            case 42: {
                this.consume();
                return this.newSymbol(199, "*");
            }
            case 47: {
                this.consume();
                return this.newSymbol(200, "/");
            }
            case 94: {
                this.consume();
                return this.newSymbol(194, "^");
            }
            case 37: {
                this.consume();
                return this.newSymbol(201, "%");
            }
            case 33: {
                if (this.peek(1) == 61) {
                    this.consume(2);
                    return this.newSymbol(183, "!=");
                }
                break;
            }
            case 61: {
                this.consume();
                if (this.peek() == 61) {
                    this.consume();
                    return this.newSymbol(186, "==");
                }
                return this.newSymbol(182, "=");
            }
            case 62: {
                this.consume();
                final int c2 = this.peek();
                switch (c2) {
                    case 61: {
                        this.consume();
                        return this.newSymbol(188, ">=");
                    }
                    case 62: {
                        this.consume();
                        if (this.peek() == 62) {
                            this.consume();
                            return this.newSymbol(192, ">>>");
                        }
                        return this.newSymbol(191, ">>");
                    }
                    default: {
                        return this.newSymbol(185, ">");
                    }
                }
            }
            case 60: {
                this.consume();
                final int c2 = this.peek();
                switch (c2) {
                    case 61: {
                        this.consume();
                        return this.newSymbol(187, "<=");
                    }
                    case 60: {
                        this.consume();
                        return this.newSymbol(190, "<<");
                    }
                    case 62: {
                        this.consume();
                        return this.newSymbol(189, "<>");
                    }
                    default: {
                        return this.newSymbol(184, "<");
                    }
                }
            }
            case 39: {
                return this.getStringLiteral(c, 225, "string literal", "'");
            }
            case 34: {
                return this.getStringLiteral(c, 226, "string literal", "\"");
            }
            case 46: {
                if (Character.digit(this.peek(1), 10) != -1) {
                    final Symbol num = this.getNumericLiteral(c);
                    return num;
                }
                this.consume();
                return this.newSymbol(205, "DOT");
            }
        }
        if (Character.isJavaIdentifierStart(c)) {
            return this.getIdentifier(c);
        }
        if (Character.isDigit(c)) {
            final Symbol num = this.getNumericLiteral(c);
            return num;
        }
        throw this.scanError("Illegal character <" + (char)c + "> code [" + c + "]");
    }
    
    private Symbol getIdentifier(int c) throws IOException {
        final StringBuffer sb = new StringBuffer().append((char)c);
        this.consume();
        while (true) {
            c = this.peek();
            if (!Character.isJavaIdentifierPart(c)) {
                break;
            }
            sb.append((char)c);
            this.consume();
        }
        final String s = sb.toString();
        final Integer index = Lexer.keywordsMap.get(s);
        if (index == null) {
            return this.add2indexLeaf(new Symbol(227, (Object)s));
        }
        return this.newSymbol(index, s);
    }
    
    private Symbol getNumericLiteral(int c) throws IOException {
        if (c == 48) {
            final int c2 = this.peek(1);
            if (".eEfFdD".indexOf(c2) == -1) {
                if (c2 == 120 || c2 == 88) {
                    this.consume(2);
                    final String num = this.getDigits(16);
                    return this.getIntegerLiteral(num, 16);
                }
                if (c2 == 98 || c2 == 66) {
                    this.consume(2);
                    final String num = this.getDigits(2);
                    return this.getIntegerLiteral(num, 2);
                }
                final String num = this.getDigits(8);
                return this.getIntegerLiteral(num, 8);
            }
        }
        final StringBuffer sb = new StringBuffer(this.getDigits(10));
        boolean hasDot = false;
        c = this.peek();
        if (c == 46) {
            hasDot = true;
            this.consume();
            sb.append((char)c).append(this.getDigits(10));
        }
        boolean hasExp = false;
        c = this.peek();
        if (c == 101 || c == 69) {
            hasExp = true;
            this.consume();
            sb.append((char)c);
            c = this.peek();
            if (c == 43 || c == 45) {
                this.consume();
                sb.append((char)c);
            }
            sb.append(this.getDigits(10));
        }
        final String num2 = sb.toString();
        try {
            switch (this.peek()) {
                case 70:
                case 102: {
                    this.consume();
                    final Float f = new Float(Float.parseFloat(num2));
                    return this.add2indexLeaf(new Symbol(231, (Object)f));
                }
                case 68:
                case 100: {
                    this.consume();
                    final Double d = new Double(Double.parseDouble(num2));
                    return this.add2indexLeaf(new Symbol(232, (Object)d));
                }
                default: {
                    if (hasDot || hasExp) {
                        final Double d = new Double(Double.parseDouble(num2));
                        return this.add2indexLeaf(new Symbol(232, (Object)d));
                    }
                    return this.getIntegerLiteral(num2, 10);
                }
            }
        }
        catch (NumberFormatException e) {
            throw this.scanError("Illegal floating-point number");
        }
    }
    
    private Symbol getIntegerLiteral(final String num, final int radix) throws IOException {
        final int c = this.peek();
        if (c == 108 || c == 76) {
            this.consume();
            try {
                final Long l = new Long(Long.parseLong(num, radix));
                return this.add2indexLeaf(new Symbol(230, (Object)l));
            }
            catch (NumberFormatException e) {
                throw this.scanError("Too big long constant");
            }
        }
        try {
            final Integer i = new Integer(Integer.parseInt(num, radix));
            return this.add2indexLeaf(new Symbol(229, (Object)i));
        }
        catch (NumberFormatException e) {
            throw this.scanError("Too big integer constant");
        }
    }
    
    private String getDigits(final int radix) throws IOException {
        final StringBuffer sb = new StringBuffer();
        while (true) {
            final int c = this.peek();
            if (c == -1 || Character.digit(c, radix) == -1) {
                break;
            }
            this.consume();
            sb.append((char)c);
        }
        return sb.toString();
    }
    
    private Symbol getFileName(final int openquote, final int kind, final String what, final char term) throws IOException {
        final StringBuffer val = new StringBuffer();
        while (true) {
            final int c = this.peek();
            if (term == c) {
                break;
            }
            if (c == openquote) {
                this.consume();
                break;
            }
            switch (c) {
                case -1: {
                    throw this.unterminatedTokenError(what, "" + term);
                }
                case 92: {
                    this.consume();
                    val.append(this.getEscapeSequence());
                    continue;
                }
                default: {
                    this.consume();
                    val.append((char)c);
                    continue;
                }
            }
        }
        return this.add2indexLeaf(new Symbol(kind, (Object)val.toString()));
    }
    
    private Symbol getTripleQuoteStringLiteral(final int openquote, final int kind, final String what, final String term) throws IOException {
        final StringBuffer val = new StringBuffer();
        boolean firstLine = true;
        while (true) {
            final int c = this.peek();
            if (c == openquote && this.peek(1) == openquote && this.peek(2) == openquote) {
                this.consume(3);
                return this.add2indexLeaf(new Symbol(kind, (Object)val.toString()));
            }
            if (c == -1) {
                throw this.unterminatedTokenError(what, term);
            }
            this.consume();
            val.append((char)c);
            if (!firstLine || c != 10) {
                continue;
            }
            firstLine = false;
            if (!val.toString().trim().isEmpty()) {
                continue;
            }
            val.setLength(0);
        }
    }
    
    private Symbol getStringLiteral(final int openquote, final int kind, final String what, final String term) throws IOException {
        this.consume();
        if (this.peek() == openquote && this.peek(1) == openquote) {
            this.consume(2);
            return this.getTripleQuoteStringLiteral(openquote, kind, what, term);
        }
        final StringBuffer val = new StringBuffer();
        while (true) {
            final int c = this.peek();
            if (c == openquote) {
                this.consume();
                return this.add2indexLeaf(new Symbol(kind, (Object)val.toString()));
            }
            switch (c) {
                case -1: {
                    throw this.unterminatedTokenError(what, term);
                }
                case 92: {
                    this.consume();
                    val.append(this.getEscapeSequence());
                    continue;
                }
                case 10: {
                    if (!this.acceptNewLineInStringLiteral) {
                        throw this.scanError("Invalid " + what);
                    }
                    break;
                }
            }
            this.consume();
            val.append((char)c);
        }
    }
    
    private char getEscapeSequence() throws IOException {
        final int c = this.peek();
        switch (c) {
            case 98: {
                this.consume();
                return '\b';
            }
            case 116: {
                this.consume();
                return '\t';
            }
            case 110: {
                this.consume();
                return '\n';
            }
            case 102: {
                this.consume();
                return '\f';
            }
            case 114: {
                this.consume();
                return '\r';
            }
            case 34: {
                this.consume();
                return '\"';
            }
            case 39: {
                this.consume();
                return '\'';
            }
            case 92: {
                this.consume();
                return '\\';
            }
            case 48:
            case 49:
            case 50:
            case 51:
            case 52:
            case 53:
            case 54:
            case 55: {
                this.consume();
                return (char)this.getOctal(c);
            }
            default: {
                throw this.scanError("Invalid escape sequence");
            }
        }
    }
    
    private int getOctal(final int first_digit) throws IOException {
        int val = Character.digit(first_digit, 8);
        while (true) {
            final int c = this.peek();
            final int d;
            if (c == -1 || (d = Character.digit(c, 8)) == -1) {
                return val;
            }
            this.consume();
            val = (val << 3) + d;
            if (val > 255) {
                throw this.scanError("Invalid octal escape sequence");
            }
        }
    }
    
    public void scanErrorMsg(final String msg, final Symbol info) {
    }
    
    private Symbol newSymbol(final int id, final String sym) {
        return new Symbol(id, (Object)sym);
    }
    
    public Symbol next_token() throws IOException {
        return this.nextToken();
    }
    
    private int getLineNumber(final int pos) {
        final int buflen = this.buf.length();
        final int p = (pos >= buflen) ? buflen : pos;
        int newlinecount = 0;
        final char[] chars = new char[p];
        this.buf.getChars(0, p, chars, 0);
        for (int i = 0; i < p; ++i) {
            if (chars[i] == '\n') {
                ++newlinecount;
            }
        }
        return newlinecount + 1;
    }
    
    private int indexOfNewLineBeforeBegin(final int begin) {
        if (begin == 0) {
            return 0;
        }
        final int newlinepos1 = this.buf.lastIndexOf("\n", begin - 1);
        final int newlinepos2 = this.buf.lastIndexOf("\r", begin - 1);
        final int newlinepos3 = Math.max(newlinepos1, newlinepos2);
        return newlinepos3 + 1;
    }
    
    private int indexOfNewLineAfterEnd(final int end) {
        final int newlinepos1 = this.buf.indexOf("\n", end);
        final int newlinepos2 = this.buf.indexOf("\r", end);
        final int newline = Math.max(newlinepos1, newlinepos2);
        if (newline == -1) {
            return this.buf.length();
        }
        return newline;
    }
    
    private String makeErrorText(final int begin, final int end) {
        int b = this.indexOfNewLineBeforeBegin(begin);
        final int e = this.indexOfNewLineAfterEnd(end);
        final int errorlinenum = this.getLineNumber(begin);
        final String linenum = "line:" + errorlinenum + " ";
        final String lineWithError = this.buf.substring(b, e);
        StringBuilder text = null;
        if (this.uiStyleErrors) {
            String HTMLFormattedString = lineWithError.substring(0, begin - b) + "<mark>" + lineWithError.substring(begin - b, end - b) + "</mark>" + lineWithError.substring(e - end, lineWithError.length());
            HTMLFormattedString = HTMLFormattedString.replaceAll("\u21b5", "<br />");
            text = new StringBuilder(HTMLFormattedString);
        }
        else {
            text = new StringBuilder(linenum + lineWithError).append('\n');
            for (int i = 0; i < linenum.length(); ++i) {
                text.append(' ');
            }
            while (b < begin) {
                text.append(' ');
                ++b;
            }
            while (b < end) {
                text.append('^');
                ++b;
            }
            if (begin == end) {
                text.append('^');
            }
            text.append('\n');
        }
        return text.toString();
    }
    
    private CompilationException unterminatedTokenError(final String s, final String term) {
        final int e = (this.lastBeginPos == this.pos) ? this.buf.length() : this.pos;
        return new UnterminatedTokenException("Unterminated " + s + ":\n" + this.makeErrorText(this.lastBeginPos, e), term);
    }
    
    private CompilationException scanError(final String s) {
        final int e = (this.lastBeginPos == this.pos) ? this.buf.length() : this.pos;
        return new CompilationException(s + ":\n" + this.makeErrorText(this.lastBeginPos, e));
    }
    
    public void report_error(final String message, final Object obj) {
        if (obj instanceof Symbol) {
            final Symbol s = (Symbol)obj;
            final String text = (s.left != -1) ? this.makeErrorText(s.left, s.right) : "";
            throw new CompilationException(message + " at:\n" + text, s.left, s.right);
        }
        throw new CompilationException(message);
    }
    
    public Symbol getSymbol(Object obj) {
        Symbol sym;
        for (sym = this.index.get(obj); sym == null && obj instanceof Expr; sym = this.index.get(obj)) {
            final Expr e = (Expr)obj;
            obj = e.originalExpr;
            if (obj == null) {
                break;
            }
        }
        return sym;
    }
    
    public String getExprText(final Object obj) {
        final Symbol s = this.getSymbol(obj);
        if (s == null) {
            return null;
        }
        try {
            return this.buf.substring(s.left, s.right);
        }
        catch (StringIndexOutOfBoundsException e) {
            return "";
        }
    }
    
    public void parseError(final String message, final Object obj) {
        if (obj == null) {
            throw new CompilationException(message);
        }
        final Symbol s = this.getSymbol(obj);
        final String text = (s != null) ? this.makeErrorText(s.left, s.right) : obj.toString();
        throw new CompilationException(message + ":\n" + text);
    }
    
    public String getBufSubstring(final int beg, final int end) {
        return this.buf.substring(beg, end);
    }
    
    public void syntaxError(final Grammar parser, final Symbol sym) {
        if (sym.left == sym.right && sym.left >= this.buf.length()) {
            int p;
            for (p = this.buf.length(); p > 0; --p) {
                final char c = this.buf.charAt(p - 1);
                if (!Character.isWhitespace(c)) {
                    break;
                }
            }
            final Symbol s = new Symbol(sym.sym, p, p, sym.value);
            this.report_error("Syntax error", s);
        }
        else {
            this.report_error("Syntax error", sym);
        }
    }
    
    public static void process(final String text) {
        final Lexer l = new Lexer(new StringReader(text));
        try {
            while (l.debug_lex()) {}
        }
        catch (Throwable e) {
            Lexer.logger.error((Object)e);
        }
    }
    
    public static boolean isKeyword(final String s) {
        return Lexer.keywordsMap.containsKey(s);
    }
    
    public static void main(final String[] args) throws IOException {
        final Lexer l = new Lexer(new StringReader("123456789\n00000000\n"));
        l.peek(37);
        process("aaaa bbbb\ncccc\nfjdhjgf !!!! fhgdjfg\n");
        process("catch if else break\n");
        process("aaaa 11123456789012345678912345666 skuzi\n");
    }
    
    static {
        Lexer.logger = Logger.getLogger((Class)Lexer.class);
        Lexer.keywordsMap = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
        for (final Field f : Sym.class.getDeclaredFields()) {
            try {
                final String name = f.getName();
                if (!name.startsWith("$")) {
                    Lexer.keywordsMap.put(name, f.getInt(null));
                }
            }
            catch (IllegalArgumentException | IllegalAccessException ex2) {
                Lexer.logger.error((Object)ex2);
            }
        }
    }
    
    public class LexerSymbolFactory extends DefaultSymbolFactory
    {
        public Symbol newSymbol(final String name, final int id, final Symbol left, final Symbol right, final Object value) {
            return Lexer.this.add2index(new Symbol(id, left, right, value));
        }
        
        public Symbol newSymbol(final String name, final int id, final Symbol left, final Symbol right) {
            return Lexer.this.add2index(new Symbol(id, left, right));
        }
        
        public Symbol newSymbol(final String name, final int id, final Object value) {
            return Lexer.this.add2index(new Symbol(id, value));
        }
        
        public Symbol newSymbol(final String name, final int id) {
            return Lexer.this.add2index(new Symbol(id));
        }
        
        public Symbol startSymbol(final String name, final int id, final int state) {
            final Symbol s = new Symbol(id);
            s.parse_state = state;
            return Lexer.this.add2index(s);
        }
    }
    
    static class FIFO<T>
    {
        private final LinkedList<T> backing;
        private final Getter<T> getter;
        
        public FIFO(final Getter<T> getter) {
            this.backing = new LinkedList<T>();
            this.getter = getter;
        }
        
        public void put(final T o) {
            this.backing.add(o);
        }
        
        public T get() throws IOException {
            if (this.backing.isEmpty()) {
                this.put(this.getter.next());
            }
            return this.backing.remove();
        }
        
        public T peek(final int i) throws IOException {
            while (i >= this.backing.size()) {
                this.put(this.getter.next());
            }
            return this.backing.get(i);
        }
        
        abstract static class Getter<T>
        {
            abstract T next() throws IOException;
        }
    }
}
