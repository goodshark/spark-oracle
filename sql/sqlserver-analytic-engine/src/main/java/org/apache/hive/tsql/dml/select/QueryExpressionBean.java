package org.apache.hive.tsql.dml.select;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.dml.mergeinto.BaseBean;
import org.apache.hive.tsql.sqlsverExcept.QuerySpecificationBean;
import org.apache.hive.tsql.sqlsverExcept.UnionBean;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by wangsm9 on 2017/4/17.
 */
public class QueryExpressionBean extends BaseBean {

    private QuerySpecificationBean querySpecificationBean;

    private List<UnionBean> unionBeanList;

    private QueryExpressionBean queryExpressionBean;


    private List<String> exceptionList = new ArrayList<>();


    private String getRunSql() {
        StringBuffer sql = new StringBuffer();
        if (unionBeanList.isEmpty()) {
            if (null != queryExpressionBean) {
                return queryExpressionBean.getSql();
            }
            if (null != querySpecificationBean) {
                return querySpecificationBean.getSql();
            }
        }
        List<UnionBean.UnionType> list = getAllUnionType();
        UnionBean.UnionType type = list.get(0);
        switch (type) {
            case UNION:
                if (null != queryExpressionBean) {
                    sql.append(queryExpressionBean.getSql());
                }
                if (null != querySpecificationBean) {
                    sql.append(querySpecificationBean.getSql());
                }
                for (UnionBean u : unionBeanList) {
                    sql.append(" union all ");
                    sql.append(u.getQuerySpecificationBean().getSql());
                }
                break;
            case EXCEPT:
            case INTERSECT:
                QuerySpecificationBean leftQueryBean = querySpecificationBean;
                leftQueryBean.setTableName(getTableNameByUUId());
                String initSql = "";
                for (UnionBean u : unionBeanList) {
                    QuerySpecificationBean rightQueryBean = u.getQuerySpecificationBean();
                    rightQueryBean.setTableName(getTableNameByUUId());
                    initSql = createExceptOrIntersectSql(leftQueryBean, rightQueryBean, type);
                    leftQueryBean.setTableName(getTableNameByUUId());
                    leftQueryBean.setSql(initSql);
                }
                sql.append(initSql);
                break;
        }
        return sql.toString();
    }

    private String getTableNameByUUId() {
        String t = "except_";
        return t + UUID.randomUUID().toString().substring(15).replaceAll("-", "_");

    }

    private String checkQuerySql(QuerySpecificationBean leftQueryBean, QuerySpecificationBean rightQueryBean) {
        if (leftQueryBean == null || rightQueryBean == null || leftQueryBean.getSelectList().size() !=
                rightQueryBean.getSelectList().size()) {
            return " sql analytic exception ...";
        }
        return "";
    }

    private String createExceptOrIntersectSql(QuerySpecificationBean leftQueryBean,
                                              QuerySpecificationBean rightQueryBean, UnionBean.UnionType unionType) {
        String exception = checkQuerySql(leftQueryBean, rightQueryBean);
        this.exceptionList.add(exception);
        String exceptOrIntersectSql = "";
        switch (unionType) {
            case EXCEPT:
                exceptOrIntersectSql = "NOT EXISTS";
                break;
            case INTERSECT:
                exceptOrIntersectSql = " EXISTS ";
                break;
        }
        StringBuffer sql = new StringBuffer();
        sql.append(" select * from (");
        sql.append(leftQueryBean.getSql());
        sql.append(") " + leftQueryBean.getTableName());
        sql.append(" WHERE  ");
        sql.append(exceptOrIntersectSql).append(Common.SPACE);

        sql.append("( ");

        sql.append(" select * from (");
        sql.append(rightQueryBean.getSql());
        sql.append(") " + rightQueryBean.getTableName());
        if (rightQueryBean.getSelectList().size() > 0 && !StringUtils.isBlank(rightQueryBean.getSelectList().get(0).getRealColumnName())) {
            sql.append(" where ");
            String leftTbName = leftQueryBean.getTableName();
            String rightTbName = rightQueryBean.getTableName();
            for (int j = 0; j < rightQueryBean.getSelectList().size(); j++) {
                if (j != 0) {
                    sql.append("  and ");
                }
                sql.append(Common.SPACE);
                sql.append(leftTbName)
                        .append(".")
                        .append(leftQueryBean.getSelectList().get(j).getRealColumnName());
                sql.append("=");
                sql.append(rightTbName)
                        .append(".")
                        .append(rightQueryBean.getSelectList().get(j).getRealColumnName());
                sql.append(Common.SPACE);
            }
        }
        sql.append(" ) ");
        return sql.toString();

    }


    private List<UnionBean.UnionType> getAllUnionType() {
        List<UnionBean.UnionType> list = new ArrayList<>();
        if (null != unionBeanList && !unionBeanList.isEmpty()) {
            for (UnionBean u : unionBeanList) {
                if (null == u) {
                    continue;
                }
                list.add(u.getUnionType());
            }
        }
        return list;
    }

    public String getExceptionInfo() {
        List<UnionBean.UnionType> list = getAllUnionType();
        StringBuffer stringBuffer = new StringBuffer();
        if (!list.isEmpty() && list.size() > 1) {
            for (UnionBean.UnionType t : list) {
                stringBuffer.append(t.toString());
                stringBuffer.append(",");
            }
        }
        if (stringBuffer.length() > 1) {
            return stringBuffer.substring(0, stringBuffer.length() - 1);
        }
        return "";
    }

    public List<String> getExceptions() {
        exceptionList.add(getExceptionInfo());
        return exceptionList;
    }


    @Override
    public String getSql() {
        return getRunSql();
    }

    public QuerySpecificationBean getQuerySpecificationBean() {
        return querySpecificationBean;
    }

    public void setQuerySpecificationBean(QuerySpecificationBean querySpecificationBean) {
        this.querySpecificationBean = querySpecificationBean;
    }

    public List<UnionBean> getUnionBeanList() {
        return unionBeanList;
    }

    public void setUnionBeanList(List<UnionBean> unionBeanList) {
        this.unionBeanList = unionBeanList;
    }

    public QueryExpressionBean getQueryExpressionBean() {
        return queryExpressionBean;
    }

    public void setQueryExpressionBean(QueryExpressionBean queryExpressionBean) {
        this.queryExpressionBean = queryExpressionBean;
    }

    public List<String> getExceptionList() {
        return exceptionList;
    }

    public void setExceptionList(List<String> exceptionList) {
        this.exceptionList = exceptionList;
    }
}
