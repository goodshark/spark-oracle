package org.apache.hive.tsql.dml.mergeinto;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.func.FuncName;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class MergeIntoStatement extends SqlStatement {
    private WithExpressionBean withExpressionBean;
    private FuncName targetTableName;
    private String targetTableAlias;

    private FuncName srcTableName;
    private String srcTableAlias;
    private String searchCondition;


    private List<MatchedStatmentBean> statmentBeanList = new ArrayList<>();
    private List<TargetNotMatcheBean> targetNotMatcheBeanArrayList = new ArrayList<>();
    private List<SourceNotMatchedBean> sourceNotMatchedBeans = new ArrayList<>();


    @Override
    public int execute() throws Exception {
        //TODO 将mergeinto拆分成多个语句执行不能保证事务性
        if (!sourceNotMatchedBeans.isEmpty()) {
            sourceNotMatched();
        }
        if (!targetNotMatcheBeanArrayList.isEmpty()) {
            executeTargetNotMatche();
        }
        if (!statmentBeanList.isEmpty()) {
            executeMatched();
        }
        return 1;
    }

    private void sourceNotMatched() {
        for (SourceNotMatchedBean bean : sourceNotMatchedBeans) {
            switch (bean.getMatchedBean().getType()) {
                case DEL:
                    StringBuffer sql = new StringBuffer();
                    sql.append(" delete ");
                    sql.append(targetTableName.getFullFuncName());
                    sql.append(Common.SPACE);

                    sql.append(" from ");
                    sql.append(srcTableName);
                    sql.append(Common.SPACE);


                    sql.append(" where ");

                    sql.append("not");
                    sql.append("(");
                    sql.append(replaceTableAlias(searchCondition));
                    sql.append(")");
                    sql.append(Common.SPACE);


                    String searchSql = bean.getSearchCondition();
                    if (!StringUtils.isBlank(searchSql)) {
                        sql.append(" and ");
                        sql.append(replaceTableAlias(searchSql));
                    }
                    commitStatement(sql.toString());
                    break;
                case UPDATE:
                    StringBuffer updateSql = new StringBuffer();
                    updateSql.append(" update ");
                    updateSql.append(targetTableName.getFullFuncName());
                    String setsql = bean.getMatchedBean().getUpdateSetSql();
                    updateSql.append(Common.SPACE);
                    updateSql.append(replaceTableAlias(setsql));
                    updateSql.append(Common.SPACE);

                    updateSql.append(" inner join ");
                    updateSql.append(srcTableName.getFuncName());
                    updateSql.append(Common.SPACE);

                    updateSql.append(" where ");

                    updateSql.append("not");
                    updateSql.append("(");
                    updateSql.append(replaceTableAlias(searchCondition));
                    updateSql.append(")");


                    String searchSqlForUp = bean.getSearchCondition();
                    if (!StringUtils.isBlank(searchSqlForUp)) {
                        updateSql.append(" and ");
                        updateSql.append(replaceTableAlias(searchSqlForUp));
                    }


                    commitStatement(updateSql.toString());
                    break;
            }
        }
    }

    private void executeTargetNotMatche() {
        for (TargetNotMatcheBean bean : targetNotMatcheBeanArrayList) {
            StringBuffer sql = new StringBuffer();
            sql.append("insert into  ").append(targetTableName.getFuncName()).append(Common.SPACE);
            sql.append(" select * from ");
            sql.append(srcTableName.getFuncName());
            sql.append(",").append(targetTableName.getFuncName());
            sql.append(Common.SPACE);
            sql.append(" where ");
            sql.append(" not");
            sql.append("(");
            sql.append(replaceTableAlias(searchCondition));
            sql.append(")");
            sql.append(Common.SPACE);
            String searchSql = bean.getSearchCondition();
            if (!StringUtils.isBlank(searchSql)) {
                sql.append(" and ");
                sql.append(replaceTableAlias(searchSql));
            }
            commitStatement(sql.toString());
        }
    }


    private void executeMatched() {
        for (MatchedStatmentBean bean : statmentBeanList) {
            switch (bean.getMatchedBean().getType()) {
                case DEL:
                    StringBuffer sql = new StringBuffer();
                    sql.append(" delete ");
                    sql.append(targetTableName.getFullFuncName());
                    sql.append(Common.SPACE);

                    sql.append(" from ");
                    sql.append(srcTableName);
                    sql.append(Common.SPACE);


                    sql.append(" where ");

                    sql.append(replaceTableAlias(searchCondition));
                    sql.append(Common.SPACE);


                    String searchSql = bean.getSearchCondition();
                    if (!StringUtils.isBlank(searchSql)) {
                        sql.append(" and ");
                        sql.append(replaceTableAlias(searchSql));
                    }
                    commitStatement(sql.toString());
                    break;
                case UPDATE:
                    StringBuffer updateSql = new StringBuffer();
                    updateSql.append(" update ");
                    updateSql.append(targetTableName.getFullFuncName());
                    String setsql = bean.getMatchedBean().getUpdateSetSql();
                    updateSql.append(Common.SPACE);
                    updateSql.append(replaceTableAlias(setsql));
                    updateSql.append(Common.SPACE);

                    updateSql.append(" inner join ");
                    updateSql.append(srcTableName.getFuncName());
                    updateSql.append(Common.SPACE);

                    updateSql.append(" where ");

                    updateSql.append(replaceTableAlias(searchCondition));


                    String searchSqlForUp = bean.getSearchCondition();
                    if (!StringUtils.isBlank(searchSqlForUp)) {
                        updateSql.append(" and ");
                        updateSql.append(replaceTableAlias(searchSqlForUp));
                    }


                    commitStatement(updateSql.toString());
                    break;
            }
        }
    }

    private String replaceTargetTableAlias(String sql) {
        //System.out.println("befor target sql ==>" + sql);
        //System.out.println("targetTableAlias ==>" + targetTableAlias + " 替换为：" + targetTableName.getFuncName());
        sql = sql.replaceAll(targetTableAlias + "\\.", " " + targetTableName.getFuncName() + "\\.");
        // System.out.println("替换结果为：" + sql);
        return sql;
    }

    private String replaceSrcTableAlias(String sql) {
       /* System.out.println("befor srctable sql ==>" + sql);
        System.out.println("targetTableAlias ==>" + srcTableAlias + " 替换为：" + srcTableName.getFuncName());*/
        sql = sql.replaceAll(srcTableAlias + "\\.", " " + srcTableName.getFuncName() + "\\.");
        //System.out.println("替换结果为：" + sql);
        return sql;
    }


    private String replaceTableAlias(String sql) {
        if (StringUtils.isBlank(sql)) {
            return "";
        }
        if (!StringUtils.isBlank(targetTableAlias)) {
            sql = replaceTargetTableAlias(sql);
        }
        if (!StringUtils.isBlank(srcTableAlias)) {
            sql = replaceSrcTableAlias(sql);
        }
        return sql;
    }


    public List<MatchedStatmentBean> getStatmentBeanList() {
        return statmentBeanList;
    }

    public void setStatmentBeanList(List<MatchedStatmentBean> statmentBeanList) {
        this.statmentBeanList = statmentBeanList;
    }

    public List<TargetNotMatcheBean> getTargetNotMatcheBeanArrayList() {
        return targetNotMatcheBeanArrayList;
    }

    public void setTargetNotMatcheBeanArrayList(List<TargetNotMatcheBean> targetNotMatcheBeanArrayList) {
        this.targetNotMatcheBeanArrayList = targetNotMatcheBeanArrayList;
    }

    public List<SourceNotMatchedBean> getSourceNotMatchedBeans() {
        return sourceNotMatchedBeans;
    }

    public void setSourceNotMatchedBeans(List<SourceNotMatchedBean> sourceNotMatchedBeans) {
        this.sourceNotMatchedBeans = sourceNotMatchedBeans;
    }

    public String getSearchCondition() {
        return searchCondition;
    }

    public void setSearchCondition(String searchCondition) {
        this.searchCondition = searchCondition;
    }

    public FuncName getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(FuncName targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getTargetTableAlias() {
        return targetTableAlias;
    }

    public void setTargetTableAlias(String targetTableAlias) {
        this.targetTableAlias = targetTableAlias;
    }

    public FuncName getSrcTableName() {
        return srcTableName;
    }

    public void setSrcTableName(FuncName srcTableName) {
        this.srcTableName = srcTableName;
    }

    public String getSrcTableAlias() {
        return srcTableAlias;
    }

    public void setSrcTableAlias(String srcTableAlias) {
        this.srcTableAlias = srcTableAlias;
    }

    public WithExpressionBean getWithExpressionBean() {
        return withExpressionBean;
    }

    public void setWithExpressionBean(WithExpressionBean withExpressionBean) {
        this.withExpressionBean = withExpressionBean;
    }
}
