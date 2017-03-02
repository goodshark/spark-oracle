package hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2016/12/29.
 */
public class LimitStatement extends SqlStatement {



    @Override
    public int execute() throws  Exception {
        SqlStatement sqlStatement =(SqlStatement)getChildrenNodes().get(0);
        sqlStatement.execute();
        try {
            Var var =(Var)sqlStatement.getRs().getObject(0);
            if(this.getNodeType().equals(Type.LIMIT_NUMBER)){
                setSql(new StringBuffer().append(Common.LIMIT).append(var.getVarValue().toString()).toString());
            }else{
                //TODO 百分比的计算，先计算count

            }
        }catch (Exception e){

        }
        return 1;


    }


}
