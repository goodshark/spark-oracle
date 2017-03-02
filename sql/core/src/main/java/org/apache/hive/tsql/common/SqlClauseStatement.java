package hive.tsql.common;

/**
 * Created by zhongdg1 on 2016/12/13.
 */
public class SqlClauseStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_SQL_CLAUSE_";

    public SqlClauseStatement() {
        super(STATEMENT_NAME);
    }

    @Override
    public int execute() {
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
