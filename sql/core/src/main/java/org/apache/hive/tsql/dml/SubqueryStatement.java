package hive.tsql.dml;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.List;

/**
 * Created by wangsm9 on 2016/12/22.
 */
public class SubqueryStatement extends SqlStatement {


    @Override
    public int execute() throws Exception {
        List<TreeNode> childrenList =getChildrenNodes();
        if(childrenList.size()!=1){
            return  0;
        }
        BaseStatement baseStatement =(BaseStatement) childrenList.get(0);
        baseStatement.execute();
        setRs(baseStatement.getRs());
        return 1;
    }





}
