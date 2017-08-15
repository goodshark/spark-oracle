package org.apache.hive.tsql.sqlsverExcept;

/**
 * Created by wangsm9 on 2017/3/29.
 */
public class UnionBean {

    public enum UnionType {
        EXCEPT, INTERSECT, UNION
    }

    private UnionType unionType;

    private QuerySpecificationBean querySpecificationBean;

    private String sql;

    public UnionType getUnionType() {
        return unionType;
    }

    public void setUnionType(UnionType unionType) {
        this.unionType = unionType;
    }

    public QuerySpecificationBean getQuerySpecificationBean() {
        return querySpecificationBean;
    }

    public void setQuerySpecificationBean(QuerySpecificationBean querySpecificationBean) {
        this.querySpecificationBean = querySpecificationBean;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
