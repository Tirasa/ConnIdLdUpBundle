/**
 * Copyright (C) 2025 ConnId (connid-dev@googlegroups.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.tirasa.connid.bundles.ldup;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectReference;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.ldaptive.BindConnectionInitializer;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.LdapEntry;
import org.ldaptive.LdapException;
import org.ldaptive.PooledConnectionFactory;
import org.ldaptive.ReturnAttributes;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.SearchScope;
import org.ldaptive.SimpleBindRequest;
import org.ldaptive.handler.ResultPredicate;
import org.ldaptive.pool.BindConnectionPassivator;

public class LdUpUtils {

    protected static final Log LOG = Log.getLog(LdUpUtils.class);

    protected static final Set<String> ENTRY_DN_ATTRS;

    static {
        Set<String> set = CollectionUtil.newCaseInsensitiveSet();
        set.add("entryDN");
        // These two are used throughout the adapter.
        set.add("dn");
        set.add("distinguishedName");
        ENTRY_DN_ATTRS = Collections.unmodifiableSet(set);
    }

    public static boolean isDNAttribute(final String attrID) {
        return ENTRY_DN_ATTRS.contains(attrID);
    }

    protected final LdUpConfiguration configuration;

    protected ConnectionConfig connectionConfig;

    protected PooledConnectionFactory connectionFactory;

    public LdUpUtils(final LdUpConfiguration configuration) {
        this.configuration = configuration;
    }

    public LdUpConfiguration getConfiguration() {
        return configuration;
    }

    public ConnectionConfig getConnectionConfig() {
        synchronized (configuration) {
            if (connectionConfig == null) {
                ConnectionConfig.Builder builder = ConnectionConfig.builder().
                        url(configuration.getUrl()).
                        useStartTLS(configuration.isUseStartTLS()).
                        autoReconnect(configuration.isAutoReconnect()).
                        connectTimeout(Duration.ofSeconds(configuration.getConnectTimeoutSeconds())).
                        responseTimeout(Duration.ofSeconds(configuration.getResponseTimeoutSeconds()));

                if (StringUtil.isNotBlank(configuration.getBindDn())) {
                    BindConnectionInitializer.Builder bcib = BindConnectionInitializer.builder().
                            dn(configuration.getBindDn());
                    Optional.ofNullable(configuration.getBindPassword()).map(SecurityUtil::decrypt).
                            ifPresent(bcib::credential);
                    builder.connectionInitializers(bcib.build());
                }

                connectionConfig = builder.build();
            }
        }
        return connectionConfig;
    }

    public PooledConnectionFactory getConnectionFactory() {
        synchronized (configuration) {
            if (connectionFactory == null) {
                connectionFactory = PooledConnectionFactory.builder().
                        config(getConnectionConfig()).
                        min(this.configuration.getPoolMinSize()).
                        max(this.configuration.getPoolMaxSize()).
                        validateOnCheckOut(true).
                        validatePeriodically(true).
                        build();

                if (StringUtil.isNotBlank(this.configuration.getBindDn())) {
                    SimpleBindRequest.Builder builder = SimpleBindRequest.builder().
                            dn(this.configuration.getBindDn());
                    Optional.ofNullable(this.configuration.getBindPassword()).map(SecurityUtil::decrypt).
                            ifPresent(builder::password);
                    connectionFactory.setPassivator(new BindConnectionPassivator(builder.build()));
                }

                connectionFactory.initialize();
            }
        }
        return connectionFactory;
    }

    public boolean isAccount(final ObjectClass objectClass) {
        return objectClass.equals(ObjectClass.ACCOUNT)
                || configuration.getAccountObjectClass().equals(objectClass.getObjectClassValue());
    }

    public boolean isGroup(final ObjectClass objectClass) {
        return objectClass.equals(ObjectClass.GROUP)
                || configuration.getGroupObjectClass().equals(objectClass.getObjectClassValue());
    }

    public String getIdAttribute(final ObjectClass objectClass) {
        if (isAccount(objectClass)) {
            return configuration.getUidAttribute();
        }
        if (isGroup(objectClass)) {
            return configuration.getGidAttribute();
        }
        return configuration.getAidAttribute();
    }

    public Optional<String> getLdapAttribute(final ObjectClass objectClass, final String attribute) {
        if (AttributeUtil.namesEqual(Uid.NAME, attribute)) {
            return Optional.of(getIdAttribute(objectClass));
        } else if (AttributeUtil.namesEqual(Name.NAME, attribute)) {
            return Optional.of("entryDN");
        } else if (OperationalAttributes.PASSWORD_NAME.equals(attribute)) {
            return Optional.of(configuration.getPasswordAttribute());
        }

        if (!AttributeUtil.isSpecialName(attribute)) {
            return Optional.of(attribute);
        }

        LOG.warn("Attribute {0} of object class {1} is not mapped to an LDAP attribute",
                attribute, objectClass.getObjectClassValue());
        return Optional.empty();
    }

    public String ldapObjectClass(final ObjectClass objectClass) {
        return ObjectClass.ACCOUNT.equals(objectClass)
                ? configuration.getAccountObjectClass()
                : ObjectClass.GROUP.equals(objectClass)
                ? configuration.getGroupObjectClass()
                : objectClass.getObjectClassValue();
    }

    public Optional<Set<String>> returnAttributes(final OperationOptions options) {
        return Optional.ofNullable(options.getAttributesToGet()).
                map(attrs -> Stream.of(attrs).
                filter(attr -> !LdUpConstants.NON_RETURN_ATTRS.contains(attr)).
                map(attr -> OperationalAttributes.PASSWORD_NAME.equals(attr)
                ? configuration.getPasswordAttribute()
                : LdUpConstants.MEMBERS_ATTR_NAME.equals(attr)
                ? configuration.getGroupMemberAttribute()
                : attr).
                collect(Collectors.toSet()));
    }

    protected void copyAttributes(
            final LdapEntry entry,
            final ConnectorObjectBuilder object,
            final Optional<Set<String>> requestedAttributes) {

        Set<String> returned = new HashSet<>();

        entry.getAttributes().forEach(attr -> {
            if (configuration.getPasswordAttribute().equals(attr.getName())) {
                object.addAttribute(AttributeBuilder.buildPassword(
                        new GuardedString(attr.getStringValue().toCharArray())));
                returned.add(OperationalAttributes.PASSWORD_NAME);
            } else if (configuration.getGroupMemberAttribute().equals(attr.getName())) {
                if (configuration.isLegacyCompatibilityMode()) {
                    object.addAttribute(AttributeBuilder.build(attr.getName(), attr.getStringValues()));
                    returned.add(attr.getName());
                } else {
                    Set<ConnectorObjectReference> members = attr.getStringValues().stream().
                            map(dn -> new ConnectorObjectReference(new ConnectorObjectBuilder().
                            setName(dn).
                            setObjectClass(new ObjectClass(configuration.getAccountObjectClass())).
                            buildIdentification())).
                            collect(Collectors.toSet());
                    object.addAttribute(AttributeBuilder.build(LdUpConstants.MEMBERS_ATTR_NAME, members));
                    returned.add(LdUpConstants.MEMBERS_ATTR_NAME);
                }
            } else {
                object.addAttribute(AttributeBuilder.build(
                        attr.getName(),
                        attr.isBinary() ? attr.getBinaryValues() : attr.getStringValues()));
                returned.add(attr.getName());
            }
        });

        requestedAttributes.ifPresent(requested -> {
            Set<String> residual = new HashSet<>(requested);
            residual.removeAll(returned);
            residual.forEach(r -> object.addAttribute(AttributeBuilder.build(r, Set.of())));
        });
    }

    protected void addAccountGroups(
            final ObjectClass objectClass,
            final String userDn,
            final ConnectorObjectBuilder user,
            final OperationOptions options) {

        // not an user, skip
        if (!isAccount(objectClass)) {
            return;
        }

        // no groups requested, skip
        if (Optional.ofNullable(options.getAttributesToGet()).
                map(attrs -> Stream.of(attrs).noneMatch(attr -> configuration.isLegacyCompatibilityMode()
                ? LdUpConstants.LEGACY_GROUPS_ATTR_NAME.equals(attr)
                : PredefinedAttributes.GROUPS_NAME.equals(attr))).
                orElse(false)) {

            return;
        }

        try {
            SearchResponse response = SearchOperation.builder().
                    factory(getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(
                            SearchRequest.builder().
                                    dn(configuration.getBaseDn()).
                                    scope(SearchScope.SUBTREE).
                                    filter("(&(objectClass=" + configuration.getGroupObjectClass() + ")"
                                            + "(" + configuration.getGroupMemberAttribute() + "=" + userDn + "))").
                                    returnAttributes(ReturnAttributes.NONE.value()).
                                    build());

            if (configuration.isLegacyCompatibilityMode()) {
                user.addAttribute(AttributeBuilder.build(
                        LdUpConstants.LEGACY_GROUPS_ATTR_NAME,
                        response.getEntryDns()));
            } else {
                Set<ConnectorObjectReference> groups = response.getEntries().stream().
                        map(LdapEntry::getDn).
                        map(dn -> new ConnectorObjectReference(new ConnectorObjectBuilder().
                        setName(dn).
                        setObjectClass(new ObjectClass(configuration.getGroupObjectClass())).
                        buildIdentification())).
                        collect(Collectors.toSet());
                user.addAttribute(AttributeBuilder.build(PredefinedAttributes.GROUPS_NAME, groups));
            }
        } catch (LdapException e) {
            LOG.error(e, "While searching groups for {0}", userDn);
        }
    }

    public ConnectorObjectBuilder connectorObjectBuilder(
            final ObjectClass objectClass,
            final Uid uid,
            final LdapEntry entry,
            final OperationOptions options) {

        ConnectorObjectBuilder object = new ConnectorObjectBuilder().
                setObjectClass(objectClass).
                setUid(uid).
                setName(entry.getDn());

        copyAttributes(entry, object, returnAttributes(options));
        addAccountGroups(objectClass, entry.getDn(), object, options);

        return object;
    }
}
