package org.apache.hive.plsql.dml.fragment.selectFragment.unpivotFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/10.
 * unpivot_in_clause
 * : IN '(' unpivot_in_elements (',' unpivot_in_elements)* ')'
 */
public class UnpivotInClauseFm extends SqlStatement {

    private List<UnpivotInElementsFm> unpivotInElementsFms;

    public void addUnpivot(UnpivotInElementsFm unpivotInElementsFm) {
        unpivotInElementsFms.add(unpivotInElementsFm);
    }


}
