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
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObjectReference;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.ldaptive.AddOperation;
import org.ldaptive.AddRequest;
import org.ldaptive.AttributeModification;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapException;
import org.ldaptive.ModifyOperation;
import org.ldaptive.ModifyRequest;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.extended.ExtendedOperation;
import org.ldaptive.extended.PasswordModifyRequest;
import org.ldaptive.handler.ResultPredicate;

public class LdUpCreateOp implements CreateOp {

    protected static final Log LOG = Log.getLog(LdUpCreateOp.class);

    protected final LdUpUtils ldUpUtils;

    public LdUpCreateOp(final LdUpUtils ldUpUtils) {
        this.ldUpUtils = ldUpUtils;
    }

    @Override
    public Uid create(
            final ObjectClass objectClass,
            final Set<Attribute> createAttributes,
            final OperationOptions options) {

        Name name = Optional.ofNullable(AttributeUtil.getNameFromAttributes(createAttributes)).
                orElseThrow(() -> new IllegalArgumentException("No Name attribute provided in the attributes"));

        List<LdapAttribute> ldapAttrs = new ArrayList<>();
        ldapAttrs.add(new LdapAttribute("objectClass", ldUpUtils.ldapObjectClass(objectClass)));

        Set<String> groups = new HashSet<>();
        Set<String> groupMembers = new HashSet<>();
        AtomicReference<String> passwordValue = new AtomicReference<>();

        createAttributes.stream().filter(a -> !CollectionUtil.isEmpty(a.getValue())).forEach(attr -> {
            if (attr.is(Name.NAME)) {
                // Handled already.
            } else if (LdUpConstants.LEGACY_GROUPS_ATTR_NAME.equals(attr.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    attr.getValue().stream().filter(Objects::nonNull).forEach(dn -> groups.add(dn.toString()));
                } else {
                    LOG.warn("Skipping {0} because legacy compatibility mode is not set", attr.getName());
                }
            } else if (PredefinedAttributes.GROUPS_NAME.equals(attr.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    LOG.warn("Skipping {0} because legacy compatibility mode is set", attr.getName());
                } else {
                    attr.getValue().stream().filter(Objects::nonNull).map(ConnectorObjectReference.class::cast).
                            forEach(member -> Optional.ofNullable(member.getValue().getAttributeByName(Name.NAME)).
                            filter(dns -> !CollectionUtil.isEmpty(dns.getValue())).
                            ifPresent(dn -> groups.add(dn.getValue().get(0).toString())));
                }
            } else if (ldUpUtils.getConfiguration().getGroupMemberAttribute().equals(attr.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    attr.getValue().stream().filter(Objects::nonNull).forEach(dn -> groupMembers.add(dn.toString()));
                } else {
                    LOG.warn("Skipping {0} because legacy compatibility mode is not set", attr.getName());
                }
            } else if (LdUpConstants.MEMBERS_ATTR_NAME.equals(attr.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    LOG.warn("Skipping {0} because legacy compatibility mode is set", attr.getName());
                } else {
                    attr.getValue().stream().filter(Objects::nonNull).map(ConnectorObjectReference.class::cast).
                            forEach(member -> Optional.ofNullable(member.getValue().getAttributeByName(Name.NAME)).
                            filter(dns -> !CollectionUtil.isEmpty(dns.getValue())).
                            ifPresent(dn -> groupMembers.add(dn.getValue().get(0).toString())));
                }
            } else if (attr.is(OperationalAttributes.PASSWORD_NAME)) {
                passwordValue.set(SecurityUtil.decrypt((GuardedString) attr.getValue().get(0)));
            } else {
                ldUpUtils.getLdapAttribute(objectClass, attr.getName()).ifPresent(ldapName -> {
                    LdapAttribute ldapAttr = new LdapAttribute(ldapName);
                    attr.getValue().stream().filter(Objects::nonNull).forEach(value -> {
                        if (value instanceof byte[]) {
                            ldapAttr.addBinaryValues(List.of((byte[]) value));
                        } else {
                            ldapAttr.addStringValues(List.of(value.toString()));
                        }
                    });
                    if (!ldapAttr.getBinaryValues().isEmpty() || !ldapAttr.getStringValues().isEmpty()) {
                        ldapAttrs.add(ldapAttr);
                    } else {
                        LOG.warn("Skipping {0} because no values were provided", attr.getName());
                    }
                });
            }
        });

        if (!groupMembers.isEmpty() && ldUpUtils.isGroup(objectClass)) {
            LdapAttribute members = new LdapAttribute(ldUpUtils.getConfiguration().getGroupMemberAttribute());
            members.addStringValues(groupMembers);
            ldapAttrs.add(members);
        }

        // 1. create
        Uid uid;
        try {
            AddOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().
                    execute(AddRequest.builder().
                            dn(name.getNameValue()).
                            attributes(ldapAttrs).
                            build());

            SearchResponse response = SearchOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(SearchRequest.objectScopeSearchRequest(
                            name.getNameValue(), new String[] { ldUpUtils.getIdAttribute(objectClass) }));
            uid = new Uid(response.getEntry().
                    getAttribute(ldUpUtils.getIdAttribute(objectClass)).getStringValue());
        } catch (LdapException e) {
            throw new ConnectorException("Create error", e);
        }

        if (ldUpUtils.isAccount(objectClass)) {
            // 2. set password if provided
            if (passwordValue.get() != null) {
                try {
                    ExtendedOperation.builder().
                            factory(ldUpUtils.getConnectionFactory()).
                            throwIf(ResultPredicate.NOT_SUCCESS).
                            build().execute(new PasswordModifyRequest(name.getNameValue(), null, passwordValue.get()));
                } catch (LdapException e) {
                    throw new ConnectorException("Set password error", e);
                }
            }

            // 3. set group memberships if provided
            for (String group : groups) {
                try {
                    ModifyOperation.builder().
                            factory(ldUpUtils.getConnectionFactory()).
                            throwIf(ResultPredicate.NOT_SUCCESS).
                            build().execute(ModifyRequest.builder().
                                    dn(group).
                                    modifications(new AttributeModification(
                                            AttributeModification.Type.ADD,
                                            new LdapAttribute(
                                                    ldUpUtils.getConfiguration().getGroupMemberAttribute(),
                                                    name.getNameValue()))).
                                    build());
                } catch (LdapException e) {
                    throw new ConnectorException("While adding " + name.getNameValue() + " to " + group, e);
                }
            }
        }

        return uid;
    }
}
