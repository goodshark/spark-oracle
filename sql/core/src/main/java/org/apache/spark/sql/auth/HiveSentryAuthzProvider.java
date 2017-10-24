package org.apache.spark.sql.auth;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.binding.hive.HiveAuthzBindingHook;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.*;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.ProviderBackend;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Created by chenfolin on 2017/10/23.
 */
public class HiveSentryAuthzProvider {

    public static final String SELECT = "SELECT";
    public static final String INSERT = "INSERT";
    public static final String SENTRY_HOOK_KEY = "hive.server2.session.hook";
    public static final String SENTRY_HOOK = "org.apache.sentry.binding.hive.HiveAuthzBindingSessionHook";

    private static final Logger LOG = LoggerFactory.getLogger(HiveSentryAuthzProvider.class);

    private static final Splitter ROLE_SET_SPLITTER = Splitter.on(",").trimResults()
            .omitEmptyStrings();

    static HiveSentryAuthzProvider provider;
    static Boolean inited = false;

    static HiveConf hiveConf;
    HiveAuthzConf authzConf;
    private final Server authServer;
    private final AuthorizationProvider authProvider;
    private ActiveRoleSet activeRoleSet;

    static{
        hiveConf = new HiveConf();
    }

    public HiveSentryAuthzProvider() {
        this.authzConf = HiveAuthzBindingHook.loadAuthzConf(hiveConf);
        this.authServer = new Server(authzConf.get(HiveAuthzConf.AuthzConfVars.AUTHZ_SERVER_NAME.getVar()));
        try {
            this.authProvider = getAuthProvider(hiveConf, authzConf, authServer.getName());
            this.activeRoleSet = parseActiveRoleSet(hiveConf.get(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET,
                    authzConf.get(HiveAuthzConf.SENTRY_ACTIVE_ROLE_SET, "")).trim());
        } catch (Exception e) {
            LOG.error("Error create HiveSentryAuthzProvider.", e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static ActiveRoleSet parseActiveRoleSet(String name)
            throws SentryUserException {
        return parseActiveRoleSet(name, null);
    }

    private static ActiveRoleSet parseActiveRoleSet(String name,
                                                    Set<TSentryRole> allowedRoles) throws SentryUserException {
        // if unset, then we choose the default of ALL
        if (name.isEmpty()) {
            return ActiveRoleSet.ALL;
        } else if (AccessConstants.NONE_ROLE.equalsIgnoreCase(name)) {
            return new ActiveRoleSet(new HashSet<String>());
        } else if (AccessConstants.ALL_ROLE.equalsIgnoreCase(name)) {
            return ActiveRoleSet.ALL;
        } else if (AccessConstants.RESERVED_ROLE_NAMES.contains(name.toUpperCase())) {
            String msg = "Role " + name + " is reserved";
            throw new IllegalArgumentException(msg);
        } else {
            if (allowedRoles != null) {
                // check if the user has been granted the role
                boolean foundRole = false;
                for (TSentryRole role : allowedRoles) {
                    if (role.getRoleName().equalsIgnoreCase(name)) {
                        foundRole = true;
                        break;
                    }
                }
                if (!foundRole) {
                    //Set the reason for hive binding to pick up
                    throw new SentryUserException("Not authorized to set role " + name, "Not authorized to set role " + name);

                }
            }
            return new ActiveRoleSet(Sets.newHashSet(ROLE_SET_SPLITTER.split(name)));
        }
    }

    public void authorize(HashSet<AuthzEntity> tables, String currentdb, String username) throws AuthorizationException{
        Subject subject = new Subject(username);
        for(AuthzEntity entity : tables){
            List<DBModelAuthorizable> inputHierarchy = new ArrayList<DBModelAuthorizable>();
            inputHierarchy.add(authServer);
            if(entity.table().database().isDefined() && entity.table().database().get() != null){
                inputHierarchy.add(new Database(entity.table().database().get()));
            } else {
                inputHierarchy.add(new Database(currentdb));
            }
            inputHierarchy.add(new Table(entity.table().table()));
            if (SELECT.equals(entity.op())) {
                if(!authProvider.hasAccess(subject, inputHierarchy, EnumSet.of(DBModelAction.SELECT), activeRoleSet)){
                    throw new AuthorizationException("User " + subject.getName() +
                            " does not have privileges for " + SELECT);
                };
            }
            if (INSERT.equals(entity.op())) {
                if(!authProvider.hasAccess(subject, inputHierarchy, EnumSet.of(DBModelAction.INSERT), activeRoleSet)){
                    throw new AuthorizationException("User " + subject.getName() +
                            " does not have privileges for " + INSERT);
                };
            }
        }
    }

    public AuthorizationProvider getAuthProvider(HiveConf hiveConf, HiveAuthzConf authzConf,
                                                        String serverName) throws Exception {
        // get the provider class and resources from the authz config
        String authProviderName = authzConf.get(HiveAuthzConf.AuthzConfVars.AUTHZ_PROVIDER.getVar());
        String resourceName =
                authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar());
        String providerBackendName = authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_BACKEND.getVar());
        String policyEngineName = authzConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar());

        LOG.debug("Using authorization provider " + authProviderName +
                " with resource " + resourceName + ", policy engine "
                + policyEngineName + ", provider backend " + providerBackendName);
        // load the provider backend class
        Constructor<?> providerBackendConstructor =
                Class.forName(providerBackendName).getDeclaredConstructor(Configuration.class, String.class);
        providerBackendConstructor.setAccessible(true);
        ProviderBackend providerBackend = (ProviderBackend) providerBackendConstructor.
                newInstance(new Object[] {authzConf, resourceName});

        // load the policy engine class
        Constructor<?> policyConstructor =
                Class.forName(policyEngineName).getDeclaredConstructor(String.class, ProviderBackend.class);
        policyConstructor.setAccessible(true);
        PolicyEngine policyEngine = (PolicyEngine) policyConstructor.
                newInstance(new Object[] {serverName, providerBackend});


        // load the authz provider class
        Constructor<?> constrctor =
                Class.forName(authProviderName).getDeclaredConstructor(String.class, PolicyEngine.class);
        constrctor.setAccessible(true);
        return (AuthorizationProvider) constrctor.newInstance(new Object[] {resourceName, policyEngine});
    }

    public static boolean useSentryAuth(){
        return SENTRY_HOOK.equals(hiveConf.get(SENTRY_HOOK_KEY));
    }

    public static  HiveSentryAuthzProvider getInstance(){
        if(provider == null || inited == false){
            synchronized (inited){
                if(inited == false){
                    provider = new HiveSentryAuthzProvider();
                    inited = true;
                }
            }
        }
        return provider;
    }

}
