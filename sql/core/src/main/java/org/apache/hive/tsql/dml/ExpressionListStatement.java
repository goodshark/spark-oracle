package org.apache.hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Row;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by wangsm9 on 2016/12/7.
 */
public class ExpressionListStatement extends SqlStatement {
    private List<TreeNode> exprs = new ArrayList<>();

    public ExpressionListStatement() {
    }

    public void addExpress(TreeNode node) {
        exprs.add(node);
    }

    public void addExpresses(List<TreeNode> nodes) {
        for (TreeNode node : nodes) {
            addExpress(node);
        }
    }

    public List<TreeNode> getExprs() {
        return exprs;
    }

    @Override
    public int execute() throws Exception {
        if (null == exprs || exprs.size() == 0) {
            return 1;
        }
        SparkResultSet rs = new SparkResultSet();
        Var exprsResults = new Var();
        exprsResults.setVarName("list");
        exprsResults.setDataType(Var.DataType.LIST);
        try {
            List<Var> listVar=new ArrayList<>();
            for (TreeNode node : exprs) {
                node.setExecSession(getExecSession());
                node.execute();
                listVar.add(node.getExpressionValue());
            }
            exprsResults.setVarValue(listVar);
            Row row = new Row(1);
            row.setValues(new Object[]{exprsResults});
            rs.addRow(row);
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            setRs(rs);
        }
        return 0;
    }


//    public class ExpressionRs extends BaseResultSet {
//        @Override
//        public Object getObject(int columnIndex) throws SQLException {
//
//            if (null == exprs || exprs.size() == 0) {
//                return null;
//            }
//
//            List<Var> exprsResults = new ArrayList<>();
//            try {
//                for (TreeNode node : exprs) {
//                    node.execute();
//                    exprsResults.add(node.getExpressionValue());
//                }
//            } catch (Exception e) {
//                throw new SQLException(e);
//            }
//            return exprsResults;
//        }
//    }

    @Override
    public String getSql() {
        if (null == exprs || exprs.size() == 0) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        for (TreeNode expr : exprs) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(expr.getSql());
        }
        return sb.toString();
    }
}
