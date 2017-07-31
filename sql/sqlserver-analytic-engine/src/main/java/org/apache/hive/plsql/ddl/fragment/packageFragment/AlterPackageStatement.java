package org.apache.hive.plsql.ddl.fragment.packageFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/28.
 * <p>
 * alter_package
 * : ALTER PACKAGE package_name COMPILE DEBUG? (PACKAGE | BODY | SPECIFICATION)? compiler_parameters_clause* (REUSE SETTINGS)? ';'
 * ;
 */
public class AlterPackageStatement extends SqlStatement {
    private PackageNameFragment packageNameFragment;

}
