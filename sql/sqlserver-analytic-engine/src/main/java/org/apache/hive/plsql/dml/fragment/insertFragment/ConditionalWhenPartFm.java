package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/11.
 * conditional_insert_when_part
 * : WHEN condition THEN multi_table_element+
 * ;
 */
public class ConditionalWhenPartFm extends SqlStatement {

    private ExpressionStatement es;
    private List<MultiTableElementFm> multiTableElementFms = new ArrayList<>();


    public void addMultiTableEleFm(MultiTableElementFm multiTableElementFm) {
        multiTableElementFms.add(multiTableElementFm);
    }

    public ExpressionStatement getEs() {
        return es;
    }

    public void setEs(ExpressionStatement es) {
        this.es = es;
    }
}
