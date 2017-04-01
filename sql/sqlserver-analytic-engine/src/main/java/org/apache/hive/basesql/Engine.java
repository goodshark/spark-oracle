package org.apache.hive.basesql;

import org.apache.hive.tsql.ExecSession;

/**
 * Created by dengrb1 on 4/1 0001.
 */
public interface Engine {

    void setSession(ExecSession session);

    void parse(String sql) throws Throwable;

    void visitTree() throws Throwable;
}
