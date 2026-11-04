package dev.streamcell.kafka;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * The authorizer of every Streamcell cluster: Kafka's {@link StandardAuthorizer} plus hard rules that
 * ACLs cannot override. See docs/adr/0010-customer-admin-principal-and-plugins.md.
 *
 * <p>Why: in Kafka a tenant that may create ACLs needs {@code ALTER} on the cluster, and that same
 * permission opens partition reassignment, leader election, feature updates, SCRAM changes and more.
 * The authorizer sees the request type, so it grants cluster {@code ALTER} to {@code CreateAcls} and
 * {@code DeleteAcls} only. Everything else is {@link #authorize} on the unchanged standard authorizer.
 *
 * <p>Super users ({@code super.users}: the platform's own principals and the brokers) bypass every rule.
 */
public class PlatformAuthorizer extends StandardAuthorizer {

    /** Resource name prefix reserved for the platform (internal topics, canary, ...). */
    public static final String RESERVED_PREFIX = "__";
    /** Strimzi's own topics, for example strimzi.cruisecontrol.metrics (no "__" prefix, found in the kind run). */
    public static final String STRIMZI_PREFIX = "strimzi.";

    /** True for names tenants may neither use nor grant access to. */
    public static boolean reserved(String name) {
        return name.startsWith(RESERVED_PREFIX) || name.startsWith(STRIMZI_PREFIX);
    }

    /** What the platform accepts as a user name (the same rule the API applies). */
    private static final java.util.regex.Pattern TENANT_NAME = java.util.regex.Pattern.compile("^[a-z][a-z0-9-]{0,30}[a-z0-9]$");

    private Set<String> superUsers = Set.of();

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        Object raw = configs.get(SUPER_USERS_CONFIG);
        Set<String> users = new HashSet<>();
        if (raw != null) {
            for (String u : raw.toString().split(";")) {
                if (!u.isBlank()) users.add(u.trim());
            }
        }
        superUsers = Set.copyOf(users);
    }

    /** For tests: set the super users without configuring the wrapped authorizer. */
    void superUsersForTest(String semicolonSeparated) {
        Set<String> users = new HashSet<>();
        for (String u : semicolonSeparated.split(";")) {
            if (!u.isBlank()) users.add(u.trim());
        }
        superUsers = Set.copyOf(users);
    }

    boolean isSuperUser(AuthorizableRequestContext ctx) {
        return superUsers.contains(ctx.principal().getPrincipalType() + ":" + ctx.principal().getName());
    }

    // ---------- authorize ----------

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext ctx, List<Action> actions) {
        if (isSuperUser(ctx)) {
            return super.authorize(ctx, actions);
        }
        AuthorizationResult[] out = new AuthorizationResult[actions.size()];
        List<Action> delegated = new ArrayList<>();
        List<Integer> index = new ArrayList<>();
        for (int i = 0; i < actions.size(); i++) {
            if (denied(ctx.requestType(), actions.get(i))) {
                out[i] = AuthorizationResult.DENIED;
            } else {
                delegated.add(actions.get(i));
                index.add(i);
            }
        }
        if (!delegated.isEmpty()) {
            List<AuthorizationResult> rs = super.authorize(ctx, delegated);
            for (int j = 0; j < rs.size(); j++) out[index.get(j)] = rs.get(j);
        }
        return List.of(out);
    }

    /** True when tenants must never be allowed this, whatever ACLs exist. */
    static boolean denied(int requestType, Action action) {
        ResourcePattern r = action.resourcePattern();
        ResourceType type = r.resourceType();
        AclOperation op = action.operation();
        switch (type) {
            case CLUSTER:
                switch (op) {
                    case ALTER:
                        // Alter on the cluster is how Kafka authorizes ACL management, and also partition
                        // reassignment, leader election, log-dir moves and feature updates.
                        return requestType != ApiKeys.CREATE_ACLS.id && requestType != ApiKeys.DELETE_ACLS.id;
                    case CREATE:
                        // Cluster-level Create authorizes creating a topic of ANY name, before the per-topic
                        // check that would stop "__" names (found with the integration test). Tenants get Create
                        // through topic ACLs only.
                        return true;
                    case ALTER_CONFIGS:   // broker and cluster configuration, client quotas
                    case CLUSTER_ACTION:  // acting as a broker
                        return true;
                    default:
                        return false;
                }
            case DELEGATION_TOKEN:
            case USER: // SCRAM credentials are managed through the platform API
                return true;
            case TOPIC:
            case GROUP:
            case TRANSACTIONAL_ID:
                return reserved(r.name());
            default:
                return false;
        }
    }

    // ---------- ACL management ----------

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext ctx, List<AclBinding> bindings) {
        if (isSuperUser(ctx)) {
            return super.createAcls(ctx, bindings);
        }
        List<CompletionStage<AclCreateResult>> out = new ArrayList<>(bindings.size());
        List<AclBinding> accepted = new ArrayList<>();
        List<Integer> index = new ArrayList<>();
        for (int i = 0; i < bindings.size(); i++) {
            String why = rejectBinding(bindings.get(i));
            if (why != null) {
                out.add(CompletableFuture.completedFuture(new AclCreateResult(new ClusterAuthorizationException(why))));
            } else {
                out.add(null);
                accepted.add(bindings.get(i));
                index.add(i);
            }
        }
        if (!accepted.isEmpty()) {
            List<? extends CompletionStage<AclCreateResult>> rs = super.createAcls(ctx, accepted);
            for (int j = 0; j < rs.size(); j++) out.set(index.get(j), rs.get(j));
        }
        return out;
    }

    /** Why a tenant may not create this ACL, or null when it may. */
    String rejectBinding(AclBinding b) {
        ResourcePattern r = b.pattern();
        switch (r.resourceType()) {
            case TOPIC:
            case GROUP:
            case TRANSACTIONAL_ID:
                break;
            default:
                return "ACLs on " + r.resourceType() + " resources are managed by the platform";
        }
        if (reserved(r.name())) {
            return "resource names starting with \"" + RESERVED_PREFIX + "\" or \"" + STRIMZI_PREFIX + "\" are reserved for the platform";
        }
        if (r.patternType() == PatternType.PREFIXED && r.name().isEmpty()) {
            return "an empty prefix would match every resource";
        }
        String principal = b.entry().principal();
        if (!principal.startsWith("User:")) {
            return "only User: principals are supported";
        }
        // Tenant principals are SCRAM users ("User:name"), certificate users ("User:CN=name") and the wildcard.
        // Anything else, in particular the platform's own certificates and the brokers, is refused.
        String who = principal.substring("User:".length());
        String name = who.startsWith("CN=") ? who.substring(3) : who;
        boolean tenantName = TENANT_NAME.matcher(name).matches() && !name.startsWith("platform") && !name.startsWith("kc-");
        if (superUsers.contains(principal) || !("*".equals(who) || tenantName)) {
            return "ACLs for platform principals cannot be created";
        }
        if (!"*".equals(b.entry().host())) {
            // Brokers see the gateway's address, not the client's, so a host restriction would be a lie.
            return "host restrictions are not supported; use host \"*\"";
        }
        return null;
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext ctx, List<AclBindingFilter> filters) {
        if (isSuperUser(ctx)) {
            return super.deleteAcls(ctx, filters);
        }
        List<CompletionStage<AclDeleteResult>> out = new ArrayList<>(filters.size());
        List<AclBindingFilter> accepted = new ArrayList<>();
        List<Integer> index = new ArrayList<>();
        for (int i = 0; i < filters.size(); i++) {
            ResourceType t = filters.get(i).patternFilter().resourceType();
            if (t != ResourceType.TOPIC && t != ResourceType.GROUP && t != ResourceType.TRANSACTIONAL_ID) {
                // ANY would also match the cluster-level ACLs the platform manages.
                out.add(CompletableFuture.completedFuture(new AclDeleteResult(new ClusterAuthorizationException(
                        "a delete filter must name the resource type: topic, group or transactionalId"))));
            } else {
                out.add(null);
                accepted.add(filters.get(i));
                index.add(i);
            }
        }
        if (!accepted.isEmpty()) {
            List<? extends CompletionStage<AclDeleteResult>> rs = super.deleteAcls(ctx, accepted);
            for (int j = 0; j < rs.size(); j++) out.set(index.get(j), rs.get(j));
        }
        return out;
    }
}
