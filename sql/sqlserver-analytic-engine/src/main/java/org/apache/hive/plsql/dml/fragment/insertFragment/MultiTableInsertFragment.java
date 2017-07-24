package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.hive.plsql.dml.OracleSelectStatement;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/11.
 * <p>
 * multi_table_insert
 * : (ALL multi_table_element+ | conditional_insert_clause) select_statement
 * ;
 */
public class MultiTableInsertFragment extends SqlStatement {
    private OracleSelectStatement oracleSelectStatement;
    private List<MultiTableElementFm> multiTableElementFms = new ArrayList<>();
    private ConditionalInsertClauseFm clauseFm;

    public OracleSelectStatement getOracleSelectStatement() {
        return oracleSelectStatement;
    }

    public void setOracleSelectStatement(OracleSelectStatement oracleSelectStatement) {
        this.oracleSelectStatement = oracleSelectStatement;
    }

    public ConditionalInsertClauseFm getClauseFm() {
        return clauseFm;
    }

    public void setClauseFm(ConditionalInsertClauseFm clauseFm) {
        this.clauseFm = clauseFm;
    }

    public void addMutiTableEleFm(MultiTableElementFm mtif) {
        multiTableElementFms.add(mtif);
    }
}
