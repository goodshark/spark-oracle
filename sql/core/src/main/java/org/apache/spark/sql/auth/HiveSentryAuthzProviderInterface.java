package org.apache.spark.sql.auth;

import org.apache.hadoop.hive.ql.metadata.AuthorizationException;

import java.util.HashSet;

/**
 * Created by chenfolin on 2017/10/25.
 */
public interface HiveSentryAuthzProviderInterface {

    public void authorize(HashSet<AuthzEntity> tables, String currentdb, String username) throws AuthorizationException;

}
