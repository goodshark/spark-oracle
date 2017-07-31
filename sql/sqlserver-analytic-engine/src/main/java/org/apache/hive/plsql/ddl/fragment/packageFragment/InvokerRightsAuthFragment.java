package org.apache.hive.plsql.ddl.fragment.packageFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/28.
 * invoker_rights_clause
 * : AUTHID (CURRENT_USER|DEFINER)
 * ;
 */
public class InvokerRightsAuthFragment extends SqlStatement {
    private String authid;

    public String getAuthid() {
        return authid;
    }

    public void setAuthid(String authid) {
        this.authid = authid;
    }
}
