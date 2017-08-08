package org.apache.hive.plsql.ddl.fragment.createViewFragment;

import org.apache.hive.plsql.dml.commonFragment.*;
import org.apache.hive.plsql.dml.fragment.selectFragment.QueryBlockFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.SelectElementFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/8/3.
 * create_view
 * : CREATE (OR REPLACE)? VIEW tableview_name ('('column_name?  (',' column_name )*  ')' )? AS subquery
 * ;
 */
public class OracleCreateViewStatment extends SqlStatement {

    private boolean replace;
    private TableViewNameFragment tableViewNameFragment;
    private SubqueryFragment subqueryFragment;
    private List<ColumnNameFragment> columnNameFragments = new ArrayList<>();

    private String tableName;

    public void addColumnName(ColumnNameFragment columnNameFragment) {
        columnNameFragments.add(columnNameFragment);
    }


    @Override
    public String getFinalSql() throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("CREATE VIEW ");
        tableName = FragMentUtils.appendOriginalSql(tableViewNameFragment, getExecSession());
        sql.append(tableName);
        sql.append(" AS ");

        if (!columnNameFragments.isEmpty()) {
            QueryBlockFragment queryBlock = subqueryFragment.getBasicElement().getQueryBlock();
            List<SelectElementFragment> selectElem = queryBlock.getSelectElements();
            if (selectElem.size() != columnNameFragments.size()) {
                throw new Exception(" sql error...");
            }
            for (int i = 0; i < selectElem.size(); i++) {
                IdFragment id = columnNameFragments.get(i).getId();
                ColumnAliasFragment aliasFragment = new ColumnAliasFragment();
                aliasFragment.setIdFragment(id);
                selectElem.get(i).setColAlias(aliasFragment);
            }
        }
        sql.append(FragMentUtils.appendOriginalSql(subqueryFragment, getExecSession()));
        return sql.toString();
    }


    @Override
    public int execute() throws Exception {
        String sql = getFinalSql();
        setRs(commitStatement(sql));
        return 0;
    }

    public boolean isReplace() {
        return replace;
    }

    public void setReplace(boolean replace) {
        this.replace = replace;
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }

    public SubqueryFragment getSubqueryFragment() {
        return subqueryFragment;
    }

    public void setSubqueryFragment(SubqueryFragment subqueryFragment) {
        this.subqueryFragment = subqueryFragment;
    }
}
