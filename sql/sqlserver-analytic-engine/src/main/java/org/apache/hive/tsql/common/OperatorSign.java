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
    COMPLEX_BOOL("bool"), CONCAT("||"), REMAINDER("remainder"), POWER("**");

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
        } else if (StringUtils.equals(val, "||")) {
            return CONCAT;
        } else if (StringUtils.equalsIgnoreCase(val, "remainder")) {
            return REMAINDER;
        } else if (StringUtils.equals(val, "**")) {
            return POWER;
        }
        return null;
    }

    public String toJavaOpString(){
        switch (this) {
            case ADD:return "+";
            case SUBTRACT:return "-";
            case MULTIPLY:return "*";
            case DIVIDE:return "/";
            case MOD:return "%";
            case BRACKET:return "()";
            case AND:return "&";
            case OR:return "|";
            case EQUAL:return "=";
            case NOT_EQUAL:return "!=";
            case GREATER_THAN:return ">";
            case NOT_GREATER_THAN:return "<=";
            case LESS_THEN:return "<";
            case NOT_LESS_THEN:return ">=";
            case XOR:return "^";
            case GREATER_THAN_OR_EQUAL:return ">=";
            case LESS_THEN_OR_EQUAL:return "<=";
            case NOT_EQUAL_ANOTHER:return "!=";
            case BIT_NOT:return "~";
            case COMPLEX_BOOL:return "bool";
            case CONCAT:return "+";
        }
        return "";
    }

    public String getOperator() {
        return operator;
    }
}
