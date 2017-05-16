package org.apache.hive.tsql.common;


import org.apache.commons.lang.StringUtils;

/**
 * Created by wangsm9 on 2016/12/5.
 */
public enum OperatorSign {
    //op=('*' | '/' | '%', '+' | '-' | '&' | '^' | '|'
    // '=' | '>' | '<' | '<' '=' | '>' '=' | '<' '>' | '!' '=' | '!' '>' | '!' '<'

    ADD("+"), SUBTRACT("-"), MULTIPLY("*"), DIVIDE("/"), MOD("%"),BRACKET("()"),
    AND("&"), OR("|"), EQUAL("="), NOT_EQUAL("!="), GREATER_THAN(">"),
    NOT_GREATER_THAN("!>"), LESS_THEN("<"), NOT_LESS_THEN("!<"),XOR("^"),
    GREATER_THAN_OR_EQUAL(">="), LESS_THEN_OR_EQUAL("<="), NOT_EQUAL_ANOTHER("<>"), BIT_NOT("~"),
    COMPLEX_BOOL("bool");

    private String operator;

    OperatorSign(String operator) {
        this.operator = operator;
    }

    public static OperatorSign getOpator(String val) {
        String v = val.trim();
        if (StringUtils.equals(val, "+")) {
            return ADD;
        } else if (StringUtils.equals(val, "-")) {
            return SUBTRACT;
        } else if (StringUtils.equals(val, "*")) {
            return MULTIPLY;
        } else if (StringUtils.equals(val, "/")) {
            return DIVIDE;
        } else if (StringUtils.equals(val, "%")) {
            return MOD;
        } else if (StringUtils.equals(val, "&")) {
            return AND;
        } else if (StringUtils.equals(val, "|")) {
            return OR;
        } else if (StringUtils.equals(val, "=")) {
            return EQUAL;
        } else if (StringUtils.equals(val, "!=")) {
            return NOT_EQUAL;
        } else if (StringUtils.equals(val, ">")) {
            return GREATER_THAN;
        } else if (StringUtils.equals(val, "!>")) {
            return NOT_GREATER_THAN;
        } else if (StringUtils.equals(val, "<")) {
            return LESS_THEN;
        } else if (StringUtils.equals(val, "!<")) {
            return NOT_LESS_THEN;
        } else if (StringUtils.equals(val, ">=")) {
            return GREATER_THAN_OR_EQUAL;
        } else if (StringUtils.equals(val, "<=")) {
            return LESS_THEN_OR_EQUAL;
        } else if (StringUtils.equals(val, "<>")) {
            return NOT_EQUAL_ANOTHER;
        }else if (StringUtils.equals(val, "^")) {
            return XOR;
        }
        return null;
    }

}
