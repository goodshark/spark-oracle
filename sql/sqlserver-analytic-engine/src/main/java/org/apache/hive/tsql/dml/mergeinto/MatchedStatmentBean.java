package org.apache.hive.tsql.dml.mergeinto;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class MatchedStatmentBean extends  BaseBean {
    private String searchCondition;

    private MatchedBean matchedBean ;


    public String getSearchCondition() {
        return searchCondition;
    }

    public void setSearchCondition(String searchCondition) {
        this.searchCondition = searchCondition;
    }

    public MatchedBean getMatchedBean() {
        return matchedBean;
    }

    public void setMatchedBean(MatchedBean matchedBean) {
        this.matchedBean = matchedBean;
    }
}
