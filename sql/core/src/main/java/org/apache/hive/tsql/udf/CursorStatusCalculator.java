package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cursor.Cursor;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by zhongdg1 on 2017/1/22.
 */
public class CursorStatusCalculator extends BaseCalculator {

    public CursorStatusCalculator() {
        setMinMax(2);
    }

    @Override
    public Var compute() throws Exception {
        String scope = StrUtils.trimQuot(getArguments(0).getString()).toUpperCase();
        String cursorName = StrUtils.trimQuot(getArguments(1).getString());
        Cursor cursor = null;
        if ("LOCAL".equals(scope)) {
            cursor = getExecSession().getVariableContainer().findCursor(cursorName, false);
        } else if ("GLOBAL".equals(scope)) {
            cursor = getExecSession().getVariableContainer().findCursor(cursorName, true);
        } else {
            throw new IllegalArgumentException("Function [CURSOR_STATUS] first argument must be local/global");
        }
        Var ret = new Var(-3, Var.DataType.INT);
        if (null == cursor) {
            return ret;
        }

        if (Cursor.CursorStatus.OPENING != cursor.getStatus() && Cursor.CursorStatus.FETCHING != cursor.getStatus()) {
            ret.setVarValue(-1);
            return ret;
        }

        if (null == cursor.getRs() || cursor.getRs().getRow() == 0) {
            ret.setVarValue(0);
            return ret;
        }
        ret.setVarValue(1);
        return ret;
    }
}
