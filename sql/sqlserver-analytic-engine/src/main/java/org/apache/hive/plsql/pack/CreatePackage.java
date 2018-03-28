package org.apache.hive.plsql.pack;

import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dbservice.ProcService;

import java.util.ArrayList;
import java.util.List;

public class CreatePackage extends TreeNode {
    private final int DB_TYPE = 5;
    private String packageName;
    private boolean isBody = false;
    private boolean replace = false;
    private List<TreeNode> packageBlocks = new ArrayList<>();
    private String packageSql;
    private String md5;

    public void setReplace() {
        replace = true;
    }

    public void setPackageBody() {
        isBody = true;
    }

    public boolean isBody() {
        return isBody;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String name) {
        packageName = name;
    }

    public void addPackageBlock(TreeNode node) {
        packageBlocks.add(node);
    }

    public List<TreeNode> getPackageBlocks() {
        return packageBlocks;
    }

    public void setPackageSql(String str) {
        packageSql = str;
    }

    public String getPackageSql() {
        return packageSql;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    @Override
    public int execute() throws Exception {
        // store package into database
        if (packageBlocks == null || packageBlocks.size() == 0)
            return 0;
        ProcService procService = new ProcService(getExecSession().getSparkSession());
        procService.createPackageObj(this, replace);
        return 0;
    }
}
