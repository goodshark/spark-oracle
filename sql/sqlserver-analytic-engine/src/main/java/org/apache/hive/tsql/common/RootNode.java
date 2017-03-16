package org.apache.hive.tsql.common;

/**
 * Created by zhongdg1 on 2016/12/20.
 */
public class RootNode extends TreeNode {
    private static final String STATEMENT_NAME = "_ROOT_";

    @Override
    public int execute() {
        return 0;
    }
}
