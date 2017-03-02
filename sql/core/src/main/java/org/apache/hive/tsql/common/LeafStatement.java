package hive.tsql.common;

/**
 * Created by zhongdg1 on 2016/12/15.
 */
public abstract class LeafStatement extends BaseStatement{
    private String sql;

    public LeafStatement(String nodeName, String sql) {
        super(nodeName); //leaf statement默认为可执行的
        super.setExecutable(true);
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
