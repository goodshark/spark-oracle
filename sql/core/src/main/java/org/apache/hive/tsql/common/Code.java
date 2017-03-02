package hive.tsql.common;

/**
 * Created by wangsm9 on 2016/11/24.
 */
public enum Code {
    SUCCESS("S", 1), FAIL("F", 2);
    private String msg;
    private int value;

    Code(String msg, int value) {
        this.msg = msg;
        this.value = value;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.msg + "_" + this.value;
    }
}
