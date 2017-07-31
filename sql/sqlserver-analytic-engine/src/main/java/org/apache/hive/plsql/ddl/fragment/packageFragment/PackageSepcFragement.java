package org.apache.hive.plsql.ddl.fragment.packageFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/28.
 * package_spec
 * : package_name invoker_rights_clause? (IS | AS) package_obj_spec* END package_name?
 * ;
 */
public class PackageSepcFragement extends SqlStatement {
    private PackageNameFragment packageNameFragment;
    private InvokerRightsAuthFragment invokerRightsAuthFragment;
    private List<PackageObjSpecFragment> packageObjSpecFragments;

    public void addPackageObj(PackageObjSpecFragment packageObjSpecFragment) {
        packageObjSpecFragments.add(packageObjSpecFragment);
    }


    public PackageNameFragment getPackageNameFragment() {
        return packageNameFragment;
    }

    public void setPackageNameFragment(PackageNameFragment packageNameFragment) {
        this.packageNameFragment = packageNameFragment;
    }

    public InvokerRightsAuthFragment getInvokerRightsAuthFragment() {
        return invokerRightsAuthFragment;
    }

    public void setInvokerRightsAuthFragment(InvokerRightsAuthFragment invokerRightsAuthFragment) {
        this.invokerRightsAuthFragment = invokerRightsAuthFragment;
    }


}
