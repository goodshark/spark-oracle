package org.apache.hive.tsql.dml.mergeinto;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class TargetNotMatcheBean extends BaseBean {
    private String searchCondition;
    private MergeNotMatchedBean mergeNotMatchedBean;


    public String getSearchCondition() {
        return searchCondition;
    }

    public void setSearchCondition(String searchCondition) {
        this.searchCondition = searchCondition;
    }

    public MergeNotMatchedBean getMergeNotMatchedBean() {
        return mergeNotMatchedBean;
    }

    public void setMergeNotMatchedBean(MergeNotMatchedBean mergeNotMatchedBean) {
        this.mergeNotMatchedBean = mergeNotMatchedBean;
    }
}
