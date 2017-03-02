package org.apache.hive.tsql.common;

import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/13.
 */
public class SqlClausesStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_SQL_CLAUSES_";

    public SqlClausesStatement() {
        super(STATEMENT_NAME);
    }

    @Override
    public int execute() {
        // TODO this is a dumb node
        /*List<TreeNode> list = getChildrenNodes();
        for (TreeNode node: list) {
            ((BaseStatement) node).execute();
        }*/
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
