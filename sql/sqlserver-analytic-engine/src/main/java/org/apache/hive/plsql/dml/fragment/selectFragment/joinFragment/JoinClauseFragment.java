package org.apache.hive.plsql.dml.fragment.selectFragment.joinFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.fragment.selectFragment.QueryPartitionClauseFragement;
import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.TableRefAuxFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/4.
 * <p>
 * join_clause
 * : query_partition_clause? (CROSS | NATURAL)? (INNER | outer_join_type)?
 * JOIN table_ref_aux query_partition_clause? (join_on_part | join_using_part)*
 */
public class JoinClauseFragment extends SqlStatement {
    private QueryPartitionClauseFragement leftQueryPartitionClauseFragements;
    private QueryPartitionClauseFragement rightQueryPartitionClauseFragements;
    private String corssJoninType;
    private TableRefAuxFragment tableRefAuxFragment;
    private String outJoinType;
    private List<JoinOnPartFragment> joinOnPartFragments = new ArrayList<>();
    private List<JoinUsingPartFragment> joinUsingPartFragments = new ArrayList<>();


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        ExecSession execSession = getExecSession();
        if (null != leftQueryPartitionClauseFragements) {
            sql.append(FragMentUtils.appendOriginalSql(leftQueryPartitionClauseFragements, execSession));
        }
        if (StringUtils.isNotBlank(corssJoninType)) {
            sql.append(corssJoninType);
        }
        if (StringUtils.isNotBlank(outJoinType)) {
            sql.append(outJoinType);
        }
        sql.append(" JOIN ");
        sql.append(FragMentUtils.appendOriginalSql(tableRefAuxFragment, execSession));

        if (null != rightQueryPartitionClauseFragements) {
            sql.append(FragMentUtils.appendOriginalSql(rightQueryPartitionClauseFragements, execSession));
        }

        int i = 0;
        if (!joinOnPartFragments.isEmpty()) {
            sql.append(FragMentUtils.appendOriginalSql(joinOnPartFragments, execSession));
          /*  if (i != 0) {
                sql.append(" AND ");
            }
            String fromTabl = getLeftJoinTbName(tableRefAuxFragment);
            String joinTabl = getLeftJoinTbName(joi);
            sql.append(fromTabl)

*/
        }

        if (!joinUsingPartFragments.isEmpty()) {
            sql.append(FragMentUtils.appendOriginalSql(joinUsingPartFragments, execSession));
        }

        return sql.toString();
    }

    private String getLeftJoinTbName(TableRefAuxFragment tableRefAuxFragment) {
        String fromTabl = FragMentUtils.appendOriginalSql(tableRefAuxFragment.getTableAliasFragment(), getExecSession());

        if (StringUtils.isBlank(fromTabl)) {
            fromTabl = FragMentUtils.appendOriginalSql(tableRefAuxFragment.getDmlTableExpressionFragment().getTableViewNameFragment(), getExecSession());
        }
        return fromTabl;
    }

    public QueryPartitionClauseFragement getLeftQueryPartitionClauseFragements() {
        return leftQueryPartitionClauseFragements;
    }

    public void setLeftQueryPartitionClauseFragements(QueryPartitionClauseFragement leftQueryPartitionClauseFragements) {
        this.leftQueryPartitionClauseFragements = leftQueryPartitionClauseFragements;
    }

    public QueryPartitionClauseFragement getRightQueryPartitionClauseFragements() {
        return rightQueryPartitionClauseFragements;
    }

    public void setRightQueryPartitionClauseFragements(QueryPartitionClauseFragement rightQueryPartitionClauseFragements) {
        this.rightQueryPartitionClauseFragements = rightQueryPartitionClauseFragements;
    }

    public void addJoinOnPart(JoinOnPartFragment jopf) {
        joinOnPartFragments.add(jopf);
    }

    public void addJoinUsingPart(JoinUsingPartFragment jupf) {
        joinUsingPartFragments.add(jupf);
    }


    public String getCorssJoninType() {
        return corssJoninType;
    }

    public void setCorssJoninType(String corssJoninType) {
        this.corssJoninType = corssJoninType;
    }

    public TableRefAuxFragment getTableRefAuxFragment() {
        return tableRefAuxFragment;
    }

    public void setTableRefAuxFragment(TableRefAuxFragment tableRefAuxFragment) {
        this.tableRefAuxFragment = tableRefAuxFragment;
    }

    public String getOutJoinType() {
        return outJoinType;
    }

    public void setOutJoinType(String outJoinType) {
        this.outJoinType = outJoinType;
    }
}
