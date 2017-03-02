package hive.tsql.another;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.Set;

/**
 * Created by zhongdg1 on 2016/12/12.
 */
public class GoStatement extends BaseStatement {

    private static final String STATEMENT_NAME = "_GO_";
    private int repeat = 1;

    public GoStatement() {
        super(STATEMENT_NAME);
        setNodeType(TreeNode.Type.GO);
    }

    public void setRepeat(int n) {
        repeat = n;
    }

    public int getRepeat() {
        return repeat;
    }

    public void decRepeat() {
        repeat = repeat - 1;
    }

    @Override
    public int execute() throws Exception {
        Set<String> tableNames = getExecSession().getVariableContainer().getAllTableVarNames();
        for(String tableName : tableNames) {
            StringBuffer sb = new StringBuffer();
            sb.append("DROP TABLE ").append(findTableVarAlias(tableName));
            commitStatement(sb.toString());
        }
        if(getExecSession().isReset()) {
            super.getExecSession().getVariableContainer().resetVars();
        }


        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return this;
    }
}
