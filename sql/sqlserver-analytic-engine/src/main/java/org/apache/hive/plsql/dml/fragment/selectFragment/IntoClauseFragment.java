package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.VariableNameFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by dengrb1 on 6/9 0009.
 * into_clause
 * : INTO variable_name (',' variable_name)*
 * | BULK COLLECT INTO variable_name (',' variable_name)*
 * ;
 */
public class IntoClauseFragment extends SqlStatement {
    private String bulk;
    private List<VariableNameFragment> variableNameFragments;

    public String getBulk() {
        return bulk;
    }

    public void setBulk(String bulk) {
        this.bulk = bulk;
    }

    public void addVariableName(VariableNameFragment vnf) {
        variableNameFragments.add(vnf);
    }

    @Override
    public String getSql() {
        return "";
    }
}
