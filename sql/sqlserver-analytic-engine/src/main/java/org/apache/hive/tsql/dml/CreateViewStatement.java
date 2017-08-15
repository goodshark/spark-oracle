package org.apache.hive.tsql.dml;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TmpTableNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by wangsm9 on 2017/5/5.
 */
public class CreateViewStatement extends SqlStatement {
    private static final Logger LOG = LoggerFactory.getLogger(CreateViewStatement.class);

    private List<String> tableNames = new ArrayList<>();

    public CreateViewStatement(String name){
        super(name);
    }

    public void addTableNames(Set<String> tableNames) {
        this.tableNames.addAll(tableNames);
    }

    public void init() throws Exception {
        TmpTableNameUtils tmpTableNameUtils = new TmpTableNameUtils();
        if (!tableNames.isEmpty()) {
            Iterator<String> it = tableNames.iterator();
            while (it.hasNext()) {
                String tbName = it.next();
                if (tmpTableNameUtils.checkIsTmpTable(tbName) || tmpTableNameUtils.checkIsGlobalTmpTable(tbName)) {
                    throw new Exception("Using temporary tables to create views is not allowed.");
                }
            }
        }
    }

    @Override
    public int execute() throws Exception {
        init();
        setRs(commitStatement(getSql()));
        return 1;
    }
}
