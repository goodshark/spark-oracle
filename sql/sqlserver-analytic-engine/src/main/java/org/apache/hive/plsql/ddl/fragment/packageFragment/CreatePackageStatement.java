package org.apache.hive.plsql.ddl.fragment.packageFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/28.
 * create_package
 * : CREATE (OR REPLACE)? PACKAGE (package_spec | package_body)? ';'
 * ;
 */
public class CreatePackageStatement extends SqlStatement {

    private boolean replace;
    private PackageSepcFragement packageSepcFragement;
    private PackageBodyFragment packageBodyFragment;


    public boolean isReplace() {
        return replace;
    }

    public void setReplace(boolean replace) {
        this.replace = replace;
    }

    public PackageSepcFragement getPackageSepcFragement() {
        return packageSepcFragement;
    }

    public void setPackageSepcFragement(PackageSepcFragement packageSepcFragement) {
        this.packageSepcFragement = packageSepcFragement;
    }

    public PackageBodyFragment getPackageBodyFragment() {
        return packageBodyFragment;
    }

    public void setPackageBodyFragment(PackageBodyFragment packageBodyFragment) {
        this.packageBodyFragment = packageBodyFragment;
    }
}
