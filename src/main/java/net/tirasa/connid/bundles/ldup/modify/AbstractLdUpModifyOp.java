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
package net.tirasa.connid.bundles.ldup.modify;

import static net.tirasa.connid.bundles.ldup.modify.LdUpCreateOp.LOG;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import net.tirasa.connid.bundles.ldup.LdUpConstants;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.ConnectorObjectReference;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.ldaptive.AttributeModification;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapException;
import org.ldaptive.ModifyOperation;
import org.ldaptive.ModifyRequest;
import org.ldaptive.ReturnAttributes;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.SearchScope;
import org.ldaptive.auth.SearchDnResolver;
import org.ldaptive.auth.User;
import org.ldaptive.extended.ExtendedOperation;
import org.ldaptive.extended.PasswordModifyRequest;
import org.ldaptive.handler.ResultPredicate;

abstract class AbstractLdUpModifyOp {

    protected static class ProcessResult {

        protected final List<LdapAttribute> ldapAttrs = new ArrayList<>();

        protected final AtomicReference<String> passwordValue = new AtomicReference<>();

        protected final Set<String> groups = new HashSet<>();

    }

    protected static void process(final Set<String> groupMembers, final List<Object> values) {
        CollectionUtil.nullAsEmpty(values).stream().
                filter(Objects::nonNull).
                map(ConnectorObjectReference.class::cast).
                forEach(member -> Optional.ofNullable(member.getValue().getAttributeByName(Name.NAME)).
                filter(dns -> !CollectionUtil.isEmpty(dns.getValue())).
                ifPresent(dn -> groupMembers.add(dn.getValue().get(0).toString())));
    }

    protected static void process(final LdapAttribute ldapAttr, final List<Object> values) {
        values.stream().filter(Objects::nonNull).forEach(value -> {
            if (value instanceof byte[]) {
                ldapAttr.addBinaryValues(List.of((byte[]) value));
            } else {
                ldapAttr.addStringValues(List.of(value.toString()));
            }
        });
    }

    protected final LdUpUtils ldUpUtils;

    protected AbstractLdUpModifyOp(final LdUpUtils ldUpUtils) {
        this.ldUpUtils = ldUpUtils;
    }

    protected String findDn(final ObjectClass objectClass, final Uid uid) {
        String idAttr = ldUpUtils.getIdAttribute(objectClass);
        String dn = null;
        if (LdUpUtils.isDNAttribute(idAttr)) {
            dn = uid.getUidValue();
        } else {
            try {
                SearchDnResolver dnResolver = SearchDnResolver.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        dn(ldUpUtils.getConfiguration().getBaseDn()).
                        subtreeSearch(true).
                        filter("(" + idAttr + "={user})").
                        build();
                dn = dnResolver.resolve(new User(uid.getUidValue()));
            } catch (LdapException e) {
                throw new ConnectorException("While resolving dn for " + uid.getUidValue(), e);
            }
        }

        if (dn == null) {
            throw new UnknownUidException(uid, objectClass);
        }

        try {
            SearchResponse response = SearchOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(
                            SearchRequest.builder().
                                    dn(dn).
                                    scope(SearchScope.OBJECT).
                                    filter("objectClass=" + ldUpUtils.ldapObjectClass(objectClass)).
                                    returnAttributes(ReturnAttributes.NONE.value()).
                                    build());
            if (response.getEntries().isEmpty()) {
                throw new ConnectorException("No entry found for " + dn
                        + " and objectClass" + ldUpUtils.ldapObjectClass(objectClass));
            }
        } catch (LdapException e) {
            throw new ConnectorException("While reading " + dn, e);
        }

        return dn;
    }

    protected Uid uid(final ObjectClass objectClass, final String dn) {
        String idAttr = ldUpUtils.getIdAttribute(objectClass);
        if (LdUpUtils.isDNAttribute(idAttr)) {
            return new Uid(dn);
        }

        try {
            SearchResponse response = SearchOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(
                            SearchRequest.builder().
                                    dn(dn).
                                    scope(SearchScope.OBJECT).
                                    filter("objectClass=" + ldUpUtils.ldapObjectClass(objectClass)).
                                    returnAttributes(idAttr).
                                    build());
            if (response.getEntries().isEmpty()) {
                throw new ConnectorException("No entry found for " + dn
                        + " and objectClass" + ldUpUtils.ldapObjectClass(objectClass));
            }
            return new Uid(response.getEntry().getAttribute(idAttr).getStringValue());
        } catch (LdapException e) {
            throw new ConnectorException("While reading " + dn, e);
        }
    }

    protected Set<String> findGroups(final String dn) {
        Set<String> groups = new HashSet<>();

        try {
            SearchResponse response = SearchOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(
                            SearchRequest.builder().
                                    dn(ldUpUtils.getConfiguration().getBaseDn()).
                                    scope(SearchScope.SUBTREE).
                                    filter("(&(objectClass="
                                            + ldUpUtils.getConfiguration().getGroupObjectClass() + ")"
                                            + "(" + ldUpUtils.getConfiguration().getGroupMemberAttribute()
                                            + "=" + dn + "))").
                                    returnAttributes(ReturnAttributes.NONE.value()).
                                    build());
            groups.addAll(response.getEntryDns());
        } catch (LdapException e) {
            throw new ConnectorException("While searching for " + dn + " group memberships", e);
        }

        return groups;
    }

    protected void groupMod(final String dn, final Set<String> groups, final AttributeModification.Type modType) {
        for (String group : groups) {
            try {
                ModifyOperation.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        throwIf(ResultPredicate.NOT_SUCCESS).
                        build().execute(ModifyRequest.builder().
                                dn(group).
                                modifications(new AttributeModification(
                                        modType,
                                        new LdapAttribute(
                                                ldUpUtils.getConfiguration().getGroupMemberAttribute(),
                                                dn))).
                                build());
            } catch (LdapException e) {
                throw new ConnectorException("While performing " + modType + " for " + dn + " on " + group, e);
            }
        }
    }

    protected ProcessResult process(final ObjectClass objectClass, final Set<Attribute> attributes) {
        ProcessResult result = new ProcessResult();

        Set<String> groupMembers = new HashSet<>();

        attributes.forEach(attr -> {
            if (attr.is(Name.NAME)) {
                // Handled already.
            } else if (LdUpConstants.LEGACY_GROUPS_ATTR_NAME.equals(attr.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    CollectionUtil.nullAsEmpty(attr.getValue()).stream().
                            filter(Objects::nonNull).
                            forEach(dn -> result.groups.add(dn.toString()));
                } else {
                    LOG.warn("Skipping {0} because legacy compatibility mode is not set", attr.getName());
                }
            } else if (PredefinedAttributes.GROUPS_NAME.equals(attr.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    LOG.warn("Skipping {0} because legacy compatibility mode is set", attr.getName());
                } else {
                    process(result.groups, attr.getValue());
                }
            } else if (ldUpUtils.getConfiguration().getGroupMemberAttribute().equals(attr.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    CollectionUtil.nullAsEmpty(attr.getValue()).stream().
                            filter(Objects::nonNull).
                            forEach(dn -> groupMembers.add(dn.toString()));
                } else {
                    LOG.warn("Skipping {0} because legacy compatibility mode is not set", attr.getName());
                }
            } else if (LdUpConstants.MEMBERS_ATTR_NAME.equals(attr.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    LOG.warn("Skipping {0} because legacy compatibility mode is set", attr.getName());
                } else {
                    process(groupMembers, attr.getValue());
                }
            } else if (attr.is(OperationalAttributes.PASSWORD_NAME)) {
                if (!CollectionUtil.isEmpty(attr.getValue())) {
                    result.passwordValue.set(SecurityUtil.decrypt((GuardedString) attr.getValue().get(0)));
                }
            } else {
                ldUpUtils.getLdapAttribute(objectClass, attr.getName()).ifPresent(ldapName -> {
                    LdapAttribute ldapAttr = new LdapAttribute(ldapName);
                    result.ldapAttrs.add(ldapAttr);
                    process(ldapAttr, CollectionUtil.nullAsEmpty(attr.getValue()));
                });
            }
        });

        if (ldUpUtils.isGroup(objectClass) && !groupMembers.isEmpty()) {
            LdapAttribute members = new LdapAttribute(ldUpUtils.getConfiguration().getGroupMemberAttribute());
            members.addStringValues(groupMembers);
            result.ldapAttrs.add(members);
        }

        return result;
    }

    protected void setPassword(final String dn, final AtomicReference<String> passwordValue) {
        if (passwordValue.get() != null) {
            try {
                ExtendedOperation.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        throwIf(ResultPredicate.NOT_SUCCESS).
                        build().execute(new PasswordModifyRequest(dn, null, passwordValue.get()));
            } catch (LdapException e) {
                throw new ConnectorException("Set password error", e);
            }
        }
    }
}
