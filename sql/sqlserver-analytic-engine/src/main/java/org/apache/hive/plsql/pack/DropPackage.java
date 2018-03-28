package org.apache.hive.plsql.pack;

import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dbservice.ProcService;

public class DropPackage extends TreeNode {
    private String packageName;

    public void setPackageName(String name) {
        packageName = name;
    }

    @Override
    public int execute() throws Exception {
        ProcService procService = new ProcService(getExecSession().getSparkSession());
        procService.delPackageObj(packageName);
        return 0;
    }
}
