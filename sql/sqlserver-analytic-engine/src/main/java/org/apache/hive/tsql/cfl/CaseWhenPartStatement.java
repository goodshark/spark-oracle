package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.List;

/**
 * Created by chenfl2 on 2017/7/12.
 */
public class CaseWhenPartStatement extends BaseStatement{

    private TreeNode whenCondition;
    private String switchVar = null;

    private boolean isSimple = true;
    private boolean first = false;

    public CaseWhenPartStatement(boolean isSimple) {
        super();
        this.isSimple = isSimple;
    }

    public CaseWhenPartStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setSwitchVar(String switchVar){
        this.switchVar = switchVar;
    }

    public void setFirst(){ this.first = true;}

    public void setCondtion(TreeNode node) {
        whenCondition = node;
    }

    public TreeNode getCondition() {return whenCondition;}

    public boolean isSimple(){return isSimple;}

    public int execute() {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(){
        StringBuffer sb = new StringBuffer();
        String ifstr = null;
        if(first){
            ifstr = "if";
        } else {
            ifstr = "else if";
        }
        if(isSimple){
            if(whenCondition instanceof  BaseStatement){
                String str = ((BaseStatement)whenCondition).doCodegen();
                if(str != null && str.startsWith("\"") && str.endsWith("\"")){
                    sb.append(ifstr);
                    sb.append("(");
                    sb.append(str);
                    sb.append("equals(");
                    sb.append(switchVar);
                    sb.append("))");
                } else {
                    sb.append(ifstr);
                    sb.append("(");
                    sb.append(switchVar);
                    sb.append(CODE_EQ2);
                    sb.append(str);
                    sb.append(")");
                }
                sb.append("{");
                sb.append(CODE_LINE_END);
                List<TreeNode> childs = getChildrenNodes();
                for(TreeNode child : childs){
                    if(child instanceof BaseStatement){
                        sb.append(((BaseStatement)child).doCodegen());
                        sb.append(CODE_LINE_END);
                    }
                }
                sb.append("}");
                sb.append(CODE_LINE_END);
            }
        } else {
            if(whenCondition instanceof  BaseStatement){
                String str = ((BaseStatement)whenCondition).doCodegen();
                sb.append(ifstr);
                sb.append("(");
                sb.append(str);
                sb.append("){");
                sb.append(CODE_LINE_END);
                List<TreeNode> childs = getChildrenNodes();
                for(TreeNode child : childs){
                    if(child instanceof BaseStatement){
                        sb.append(((BaseStatement)child).doCodegen());
                        sb.append(CODE_LINE_END);
                    }
                }
                sb.append("}");
                sb.append(CODE_LINE_END);
            }
        }
        return sb.toString();
    }

}
