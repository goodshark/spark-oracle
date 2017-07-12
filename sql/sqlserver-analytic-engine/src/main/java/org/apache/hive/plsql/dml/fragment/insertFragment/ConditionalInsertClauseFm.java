package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/11.
 * conditional_insert_clause
 * : (ALL | FIRST)? conditional_insert_when_part+ conditional_insert_else_part?
 * ;
 */
public class ConditionalInsertClauseFm extends SqlStatement {

    private String all;

    public String getAll() {
        return all;
    }

    public void setAll(String all) {
        this.all = all;
    }

    public ConditionalElsePartFm getConditionalElsePartFm() {
        return conditionalElsePartFm;
    }

    public void setConditionalElsePartFm(ConditionalElsePartFm conditionalElsePartFm) {
        this.conditionalElsePartFm = conditionalElsePartFm;
    }

    private List<ConditionalWhenPartFm> conditionalWhenPartFms = new ArrayList<>();
    private ConditionalElsePartFm conditionalElsePartFm;

    public void addConditionalWhenPart(ConditionalWhenPartFm conditionalWhenPartFm) {
        conditionalWhenPartFms.add(conditionalWhenPartFm);
    }


}
