package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;
import org.datanucleus.store.types.backed.HashMap;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by dengrb1 on 2/15 0015.
 */
public class SoundexCalculator extends BaseCalculator {
    private final Map<Character, Integer> repLatter = new java.util.HashMap<Character, Integer>() {
        {
            put('B',1); put('F',1); put('P',1); put('V',1);
            put('C',2); put('G',2); put('J',2); put('K',2);
            put('Q',2); put('S',2); put('X',2); put('Z',2);
            put('D',3); put('T',3);
            put('L',4);
            put('M',5); put('N',5);
            put('R',6);
        }
    };

    public SoundexCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("soundex func", null, Var.DataType.NULL);
        StringBuilder sb = new StringBuilder();
        if (getAllArguments().size() != 1)
            throw new FunctionArgumentException("soundex", getAllArguments().size(), 1, 1);
        Var arg = getArguments(0);
        String argStr = StrUtils.trimQuot(arg.getVarValue().toString());
        if (argStr.isEmpty())
            return var;

        sb.append(argStr.substring(0, 1).toUpperCase());
        if (argStr.length() == 1) {
            var.setDataType(Var.DataType.STRING);
            var.setVarValue(sb.append("000"));
            return var;
        }

        char[] chars = argStr.substring(1).toCharArray();
        int preIn = -1, curIn = -1;
        for (int i = 0; i < chars.length; i++) {
            curIn = i;
            if (preIn == -1) {
                preIn = curIn;
                continue;
            } else if (chars[preIn] == chars[curIn]) {
                continue;
            }
            preIn++;
            if (preIn < curIn) {
                chars[preIn] = chars[curIn];
                continue;
            }
        }
        String uniqueStr = new String(Arrays.copyOfRange(chars, 0, preIn+1));
        uniqueStr = uniqueStr.toUpperCase();
        for (char ch: uniqueStr.toCharArray()) {
            if (repLatter.containsKey(ch)) {
                sb.append(Integer.toString(repLatter.get(ch)));
                if (sb.length() >= 4)
                    break;
            }
        }

        if (sb.length() < 4) {
            for (int i = sb.length(); i < 4; i++)
                sb.append("0");
        }
        String resStr = sb.substring(0, 4).toString();
        var.setDataType(Var.DataType.STRING);
        var.setVarValue(resStr);
        return var;
    }
}
