package hive.tsql.dml;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.FunctionBean;

/**
 * Created by wangsm9 on 2016/12/14.
 */
public class FunctionStatement extends BaseStatement {
    public FunctionStatement(FunctionBean functionBean) {
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
