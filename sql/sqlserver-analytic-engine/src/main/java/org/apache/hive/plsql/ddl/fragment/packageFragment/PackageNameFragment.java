package org.apache.hive.plsql.ddl.fragment.packageFragment;

import org.apache.hive.plsql.dml.commonFragment.IdFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/28.
 */
public class PackageNameFragment extends SqlStatement {

    private IdFragment idFragment;

    public IdFragment getIdFragment() {
        return idFragment;
    }

    public void setIdFragment(IdFragment idFragment) {
        this.idFragment = idFragment;
    }
}
