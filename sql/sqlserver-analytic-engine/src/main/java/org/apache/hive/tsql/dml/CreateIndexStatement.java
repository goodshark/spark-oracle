package org.apache.hive.tsql.dml;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TmpTableNameUtils;
import org.apache.hive.tsql.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by wangsm9 on 2017/5/5.
 */
public class CreateIndexStatement extends SqlStatement {
    private static final Logger LOG = LoggerFactory.getLogger(CreateIndexStatement.class);

    private String indexName;
    private String tableName;
    private List<String> cols;

    public CreateIndexStatement(String name) {
        super(name);
    }


    public void init() throws Exception {
        TmpTableNameUtils tmpTableNameUtils = new TmpTableNameUtils();
        String dbName = "";
        if (tableName.contains(".")) {
            String[] dbTb = tableName.split("\\.");
            tableName = dbTb[1];
            dbName = dbTb[0];
        }
        if (tmpTableNameUtils.checkIsGlobalTmpTable(tableName) || tmpTableNameUtils.checkIsTmpTable(tableName)) {
            tableName = getExecSession().getRealTableName(tableName);
        } else {
            if (StringUtils.isBlank(dbName)) {
                tableName = getExecSession().getDatabase() + "." + tableName;
            } else {
                tableName = dbName + "." + tableName;
            }
        }
    }

    @Override
    public int execute() throws Exception {
        init();
        StringBuffer sql = new StringBuffer();
        sql.append("CREATE INDEX ");
        sql.append(indexName);
        sql.append(Common.SPACE);
        sql.append(" ON TABLE ");
        sql.append(tableName);
        sql.append("(");
        sql.append(StrUtils.concat(cols));
        sql.append(")");
        sql.append("  AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' ");
        sql.append(" WITH DEFERRED REBUILD ");
        setRs(commitStatement(sql.toString()));
        return 1;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getCols() {
        return cols;
    }

    public void setCols(List<String> cols) {
        this.cols = cols;
    }
}
