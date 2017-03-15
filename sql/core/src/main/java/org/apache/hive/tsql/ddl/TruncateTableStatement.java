package org.apache.hive.tsql.ddl;

import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TmpTableNameUtils;

/**
 * Created by wangsm9 on 2017/2/23.
 */
public class TruncateTableStatement  extends SqlStatement {
    private String tableName;
    public  TruncateTableStatement(String tableName){
        this.tableName=tableName;
    }

    @Override
    public int execute() throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("TRUNCATE TABLE ");
        String tableAliasName = getExecSession().getRealTableName(tableName);
        sb.append(tableAliasName);
        commitStatement(sb.toString());
        return 1;
    }
}
