package hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.node.PredicateNode;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dengrb1 on 2/17 0017.
 */
public class PatIndexCalculator extends BaseCalculator {
    private int patGroupNum = 0;

    public PatIndexCalculator() {
    }

    private String transformGroup(String pat) {
        StringBuilder sb = new StringBuilder();
        if (pat.startsWith(".*")) {
            sb.append("(.*)");
            sb.append("(").append(pat.substring(2, pat.length() - 2));
            patGroupNum = 2;
        } else {
            sb.append("(").append(pat.substring(0, pat.length() - 2));
            patGroupNum = 1;
        }
        if (pat.endsWith(".*")) {
            sb.append(")").append("(.*)");
        } else {
            sb.append(pat.substring(pat.length() - 2, pat.length())).append(")");
        }
        return sb.toString();
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("patindex func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 2)
            throw new FunctionArgumentException("patindex", getAllArguments().size(), 2, 2);

        Var arg1 = getArguments(0);
        Var arg2 = getArguments(1);
        String patStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        String targetStr = StrUtils.trimQuot(arg2.getVarValue().toString());
        PredicateNode pn = new PredicateNode();
        // transform sql-server pattern into java pattern
        String javaPatStr = pn.transformPattern(patStr, null);
        // transfrom .*pattern.* -> (.*)(pattern)(.*)
        String finalPatStr = transformGroup(javaPatStr);
        int index = 0;
        Pattern pa = Pattern.compile(finalPatStr);
        Matcher matcher = pa.matcher(targetStr);
        if (matcher.matches()) {
            index = matcher.start(patGroupNum) + 1;
        }

        var.setDataType(Var.DataType.INT);
        var.setVarValue(index);
        return var;
    }
}
