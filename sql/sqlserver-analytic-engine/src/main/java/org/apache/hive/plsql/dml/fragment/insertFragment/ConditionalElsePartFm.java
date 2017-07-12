package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/11.
 * conditional_insert_else_part
 * : ELSE multi_table_element+
 * ;
 */
public class ConditionalElsePartFm extends SqlStatement {

    private List<MultiTableElementFm> multiTableElementFms = new ArrayList<>();


    public void addMultiTabelEle(MultiTableElementFm elementFm) {
        multiTableElementFms.add(elementFm);
    }
}
