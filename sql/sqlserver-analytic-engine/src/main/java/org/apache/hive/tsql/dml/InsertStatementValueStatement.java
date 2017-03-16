package org.apache.hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wangsm9 on 2016/12/15.
 */
public class InsertStatementValueStatement extends SqlStatement {

    @Override
    public int execute() throws Exception {
        List<TreeNode> list = getChildrenNodes();
        if (null == list || list.size() != 1) {
            return -1;
        }
        list.get(0).execute();
        setRs(list.get(0).getRs());
        return  0;
    }
}
