package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.util.StrUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class RankWindowFunction extends BaseFunction {

    public RankWindowFunction(FuncName name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        List<Var> results = new ArrayList<>();
        Var val = new Var(getSql(), Var.DataType.STRING);

        results.add(val);
        doCall(results);
        return 0;
    }
}
