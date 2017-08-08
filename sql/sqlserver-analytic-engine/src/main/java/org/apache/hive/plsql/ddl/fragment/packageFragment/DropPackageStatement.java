package org.apache.hive.plsql.ddl.fragment.packageFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/28.
 * <p>
 * drop_package
 * : DROP PACKAGE BODY? package_name ';'
 * ;
 */
public class DropPackageStatement extends SqlStatement {
    private boolean body;
    private PackageNameFragment packageNameFragment;

    public boolean isBody() {
        return body;
    }

    public void setBody(boolean body) {
        this.body = body;
    }

    public PackageNameFragment getPackageNameFragment() {
        return packageNameFragment;
    }

    public void setPackageNameFragment(PackageNameFragment packageNameFragment) {
        this.packageNameFragment = packageNameFragment;
    }
}
