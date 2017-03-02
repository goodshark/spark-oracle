package hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseResultSet;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2016/12/21.
 * /**
 * CASE 表达式有两种格式：
 * CASE 简单表达式，它通过将表达式与一组简单的表达式进行比较来确定结果。
 * CASE 搜索表达式，它通过计算一组布尔表达式来确定结果。
 * <p>
 * g4文件
 * | CASE caseExpr=expression switch_section+ (ELSE elseExpr=expression)? END   #case_expression
 * | CASE switch_search_condition_section+ (ELSE elseExpr=expression)? END      #case_expression
 * <p>
 * declare @a INT
 * declare @b INT
 * DECLARE @c INT
 * set @a = 9
 * print @a
 * ------搜索表达式------
 * SET @b =
 * CASE
 * -- Check for employee
 * WHEN  @a>10 THEN 100
 * else 200
 * END
 * PRINT @b
 * <p>
 * -----简单表达式------
 * set @c =
 * CASE @a
 * when 22 THEN 220
 * when 24 then 240
 * else 1000
 * end
 * PRINT @c
 */
public class CaseWhenStatement extends SqlStatement {


    private int caseWhenStatementType = 0;// 0表示简单表达式 1 表示搜索表达式

    @Override
    public int execute() throws Exception {
        setRs(new CaseWhenResultSet());
        return 0;
    }


    public class CaseWhenResultSet extends BaseResultSet {
        @Override
        public Object getObject(int columnIndex) throws SQLException {
            try {


                List<TreeNode> childrenNodes = getChildrenNodes();
                //紧跟case后面的表达式
                TreeNode caseInputNode = null;
                //else 表达式
                TreeNode elseNode = null;
                //swich表达式，包含when 和then表达式
                List<TreeNode> swichNode = new ArrayList<>();
                for (int i = 0; i < childrenNodes.size(); i++) {
                    TreeNode c = childrenNodes.get(i);
                    switch (c.getNodeType()) {
                        case CASE_INPUT:
                            caseInputNode = c;
                            break;
                        case SWICH:
                            swichNode.add(c);
                            break;
                        case ELSE:
                            elseNode = c;
                    }
                }

                if (caseWhenStatementType == 0) {
                    //简单表达式
                    caseInputNode.execute();
                    Var caseInputvar = (Var) caseInputNode.getRs().getObject(0);
                    for (TreeNode node : swichNode) {
                        List<TreeNode> swichchildrenNodes = node.getChildrenNodes();
                        if (swichchildrenNodes.size() != 2) {
                            return Var.Null;
                        }
                        if (swichchildrenNodes.get(0).getNodeType() == Type.WHEN) {
                            swichchildrenNodes.get(0).execute();
                            Var whenVar = (Var) swichchildrenNodes.get(0).getRs().getObject(0);
                            if (caseInputvar.equals(whenVar)) {
                                swichchildrenNodes.get(1).execute();
                                return swichchildrenNodes.get(1).getRs().getObject(1);
                            }
                        }
                    }
                    elseNode.execute();
                    return elseNode.getRs().getObject(0);

                } else {
                    //搜索表达式
                    for (TreeNode node : swichNode) {
                        List<TreeNode> swichchildrenNodes = node.getChildrenNodes();
                        if (swichchildrenNodes.size() != 2) {
                            return Var.Null;
                        }
                        if (swichchildrenNodes.get(0).getNodeType() != Type.THEN) {
                            swichchildrenNodes.get(0).execute();
                            boolean whenFlag = ((LogicNode) swichchildrenNodes.get(0)).getBool();
                            if (whenFlag) {
                                swichchildrenNodes.get(1).execute();
                                return swichchildrenNodes.get(1).getRs().getObject(1);
                            }
                        }
                    }
                    elseNode.execute();
                    return elseNode.getRs().getObject(0);
                }
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    public int getCaseWhenStatementType() {
        return caseWhenStatementType;
    }

    public void setCaseWhenStatementType(int caseWhenStatementType) {
        this.caseWhenStatementType = caseWhenStatementType;
    }

}
