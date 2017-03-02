package hive.tsql.exception;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/6.
 */
public class ParserErrorListener extends BaseErrorListener {
    private List<Exception> exceptions = new ArrayList<>();

    public ParserErrorListener() {
        super();
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
//        super.syntaxError(recognizer, offendingSymbol, line, charPositionInLine, msg, e);
        exceptions.add(new ParserException(new StringBuffer().append(line).append(":").append(charPositionInLine).append(" ").append(msg).toString()));
    }

    @Override
    public void reportAmbiguity(Parser recognizer, DFA dfa, int startIndex, int stopIndex, boolean exact, BitSet ambigAlts, ATNConfigSet configs) {
        super.reportAmbiguity(recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs);
    }

    @Override
    public void reportAttemptingFullContext(Parser recognizer, DFA dfa, int startIndex, int stopIndex, BitSet conflictingAlts, ATNConfigSet configs) {
        super.reportAttemptingFullContext(recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs);
    }

    @Override
    public void reportContextSensitivity(Parser recognizer, DFA dfa, int startIndex, int stopIndex, int prediction, ATNConfigSet configs) {
        super.reportContextSensitivity(recognizer, dfa, startIndex, stopIndex, prediction, configs);
    }
}
