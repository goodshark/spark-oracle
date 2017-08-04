package org.apache.hive.plsql.ddl.commonFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/8/2.
 * crud_table
 * : CLUSTERED BY '(' column_name (',' column_name)* ')' INTO  DECIMAL BUCKETS STORED AS ORC TBLPROPERTIES
 * '('TRANSACTIONAL '=' TRUE')'
 * ;
 */
public class CrudTableFragment extends SqlStatement {

    private List<ColumnNameFragment> columnNameFragments = new ArrayList<>();

    private String bucketNumber;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append("CLUSTERED BY ");
        sql.append("(");
        sql.append(FragMentUtils.appendOriginalSql(columnNameFragments, getExecSession()));
        sql.append(")");
        sql.append("INTO ");
        sql.append(bucketNumber);
        sql.append(" BUCKETS STORED AS ORC TBLPROPERTIES" +
                " (\"TRANSACTIONAL\" = \"TRUE\")");
        return sql.toString();
    }

    public String getBucketNumber() {
        return bucketNumber;
    }

    public void setBucketNumber(String bucketNumber) {
        this.bucketNumber = bucketNumber;
    }

    public void addColumnName(ColumnNameFragment columnNameFragment) {
        columnNameFragments.add(columnNameFragment);
    }
}
