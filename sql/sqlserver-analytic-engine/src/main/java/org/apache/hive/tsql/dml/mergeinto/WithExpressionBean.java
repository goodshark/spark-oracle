package org.apache.hive.tsql.dml.mergeinto;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class WithExpressionBean extends BaseBean {

    private List<CommonTableExpressionBean> list = new ArrayList<>();

    public List<CommonTableExpressionBean> getList() {
        return list;
    }

    public void setList(List<CommonTableExpressionBean> list) {
        this.list = list;
    }
}
