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
import java.util.stream.Collectors;
import net.tirasa.connid.bundles.ldup.LdUpConstants;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeDelta;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.operations.UpdateAttributeValuesOp;
import org.identityconnectors.framework.spi.operations.UpdateDeltaOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;
import org.ldaptive.AttributeModification;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapException;
import org.ldaptive.ModifyDnOperation;
import org.ldaptive.ModifyDnRequest;
import org.ldaptive.ModifyOperation;
import org.ldaptive.ModifyRequest;
import org.ldaptive.dn.Dn;
import org.ldaptive.handler.ResultPredicate;

public class LdUpUpdateOp extends AbstractLdUpModifyOp implements UpdateOp, UpdateDeltaOp, UpdateAttributeValuesOp {

    protected static class ProcessDeltaResult {

        protected final List<AttributeModification> modifications = new ArrayList<>();

        protected final AtomicReference<String> passwordValue = new AtomicReference<>();

        protected final Set<String> groupsToAdd = new HashSet<>();

        protected final Set<String> groupsToRemove = new HashSet<>();

        protected final Set<String> groupsToReplace = new HashSet<>();

    }

    protected static final Log LOG = Log.getLog(LdUpUpdateOp.class);

    public LdUpUpdateOp(final LdUpUtils ldUpUtils) {
        super(ldUpUtils);
    }

    @Override
    public Uid update(
            final ObjectClass objectClass,
            final Uid uid,
            final Set<Attribute> replaceAttributes,
            final OperationOptions options) {

        AtomicReference<String> dn = new AtomicReference<>(findDn(objectClass, uid));
        AtomicReference<String> prevDn = new AtomicReference<>();

        // extract the Name attribute, if any and other than the current dn, to be used to rename the entry later
        Optional<Name> newName = Optional.ofNullable(AttributeUtil.getNameFromAttributes(replaceAttributes)).
                filter(name -> !dn.get().equals(name.getNameValue()));

        Set<Attribute> updateAttributes = new HashSet<>(replaceAttributes);
        newName.ifPresent(updateAttributes::remove);

        ProcessResult result = process(objectClass, updateAttributes);

        Set<String> groupsBefore = ldUpUtils.isAccount(objectClass)
                ? findGroups(dn.get())
                : Set.of();

        // 1. update
        if (!result.ldapAttrs.isEmpty()) {
            try {
                ModifyOperation.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        throwIf(ResultPredicate.NOT_SUCCESS).
                        build().execute(ModifyRequest.builder().
                                dn(dn.get()).
                                modifications(result.ldapAttrs.stream().
                                        map(ldapAttr -> new AttributeModification(
                                        AttributeModification.Type.REPLACE, ldapAttr)).
                                        collect(Collectors.toList())).
                                build());
            } catch (LdapException e) {
                throw new ConnectorException("Update error", e);
            }
        }

        // 2. rename
        newName.ifPresent(name -> {
            Dn newDn = new Dn(name.getNameValue());
            try {
                ModifyDnOperation.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        throwIf(ResultPredicate.NOT_SUCCESS).
                        build().execute(ModifyDnRequest.builder().
                                oldDN(dn.get()).
                                newRDN(newDn.getRDn().format()).
                                superior(newDn.getParent().format()).
                                delete(true).
                                build());

                prevDn.set(dn.get());
                dn.set(name.getNameValue());
            } catch (LdapException e) {
                throw new ConnectorException("Rename error from " + dn + " to " + name, e);
            }
        });

        if (ldUpUtils.isAccount(objectClass)) {
            // 3. set password if provided
            setPassword(dn.get(), result.passwordValue);

            // 4. set group memberships if provided
            Set<String> groupsToAdd;
            Set<String> groupsToRemove;
            if (prevDn.get() == null) {
                groupsToAdd = new HashSet<>(result.groups);
                groupsToAdd.removeAll(groupsBefore);
                groupsToRemove = new HashSet<>(groupsBefore);
                groupsToRemove.removeAll(result.groups);
            } else {
                groupsToAdd = result.groups;
                groupsToRemove = groupsBefore;
            }

            groupMod(dn.get(), groupsToAdd, AttributeModification.Type.ADD);
            groupMod(Optional.ofNullable(prevDn.get()).orElse(dn.get()),
                    groupsToRemove, AttributeModification.Type.DELETE);
        }

        return uid(objectClass, dn.get());
    }

    protected ProcessDeltaResult processDelta(final ObjectClass objectClass, final Set<AttributeDelta> modifications) {
        ProcessDeltaResult result = new ProcessDeltaResult();

        Set<String> groupMembersToAdd = new HashSet<>();
        Set<String> groupMembersToRemove = new HashSet<>();
        Set<String> groupMembersToReplace = new HashSet<>();

        modifications.forEach(attrDelta -> {
            if (attrDelta.is(Uid.NAME) || attrDelta.is(Name.NAME)) {
                throw new IllegalArgumentException("Do not perform rename via updateDelta, use standard update");
            } else if (LdUpConstants.LEGACY_GROUPS_ATTR_NAME.equals(attrDelta.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    CollectionUtil.nullAsEmpty(attrDelta.getValuesToAdd()).stream().
                            filter(Objects::nonNull).
                            forEach(dn -> result.groupsToAdd.add(dn.toString()));
                    CollectionUtil.nullAsEmpty(attrDelta.getValuesToRemove()).stream().
                            filter(Objects::nonNull).
                            forEach(dn -> result.groupsToRemove.add(dn.toString()));
                    CollectionUtil.nullAsEmpty(attrDelta.getValuesToReplace()).stream().
                            filter(Objects::nonNull).
                            forEach(dn -> result.groupsToReplace.add(dn.toString()));
                } else {
                    LOG.warn("Skipping {0} because legacy compatibility mode is not set", attrDelta.getName());
                }
            } else if (PredefinedAttributes.GROUPS_NAME.equals(attrDelta.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    LOG.warn("Skipping {0} because legacy compatibility mode is set", attrDelta.getName());
                } else {
                    process(result.groupsToAdd, attrDelta.getValuesToAdd());
                    process(result.groupsToRemove, attrDelta.getValuesToRemove());
                    process(result.groupsToReplace, attrDelta.getValuesToReplace());
                }
            } else if (ldUpUtils.getConfiguration().getGroupMemberAttribute().equals(attrDelta.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    CollectionUtil.nullAsEmpty(attrDelta.getValuesToAdd()).stream().
                            filter(Objects::nonNull).
                            forEach(dn -> groupMembersToAdd.add(dn.toString()));
                    CollectionUtil.nullAsEmpty(attrDelta.getValuesToRemove()).stream().
                            filter(Objects::nonNull).
                            forEach(dn -> groupMembersToRemove.add(dn.toString()));
                    CollectionUtil.nullAsEmpty(attrDelta.getValuesToReplace()).stream().
                            filter(Objects::nonNull).
                            forEach(dn -> groupMembersToReplace.add(dn.toString()));
                } else {
                    LOG.warn("Skipping {0} because legacy compatibility mode is not set", attrDelta.getName());
                }
            } else if (LdUpConstants.MEMBERS_ATTR_NAME.equals(attrDelta.getName())) {
                if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                    LOG.warn("Skipping {0} because legacy compatibility mode is set", attrDelta.getName());
                } else {
                    process(groupMembersToAdd, attrDelta.getValuesToAdd());
                    process(groupMembersToRemove, attrDelta.getValuesToRemove());
                    process(groupMembersToReplace, attrDelta.getValuesToReplace());
                }
            } else if (attrDelta.is(OperationalAttributes.PASSWORD_NAME)) {
                if (!CollectionUtil.isEmpty(attrDelta.getValuesToReplace())) {
                    result.passwordValue.set(
                            SecurityUtil.decrypt((GuardedString) attrDelta.getValuesToReplace().get(0)));
                }
            } else {
                ldUpUtils.getLdapAttribute(objectClass, attrDelta.getName()).ifPresent(ldapName -> {
                    if (!CollectionUtil.isEmpty(attrDelta.getValuesToAdd())) {
                        LdapAttribute ldapAttr = new LdapAttribute(ldapName);
                        process(ldapAttr, attrDelta.getValuesToAdd());
                        result.modifications.add(
                                new AttributeModification(AttributeModification.Type.ADD, ldapAttr));
                    }

                    if (!CollectionUtil.isEmpty(attrDelta.getValuesToRemove())) {
                        LdapAttribute ldapAttr = new LdapAttribute(ldapName);
                        process(ldapAttr, attrDelta.getValuesToRemove());
                        result.modifications.add(
                                new AttributeModification(AttributeModification.Type.DELETE, ldapAttr));
                    }

                    if (!CollectionUtil.isEmpty(attrDelta.getValuesToReplace())) {
                        LdapAttribute ldapAttr = new LdapAttribute(ldapName);
                        process(ldapAttr, attrDelta.getValuesToReplace());
                        result.modifications.add(
                                new AttributeModification(AttributeModification.Type.REPLACE, ldapAttr));
                    }
                });
            }
        });

        if (ldUpUtils.isGroup(objectClass)) {
            if (!groupMembersToAdd.isEmpty()) {
                LdapAttribute members = new LdapAttribute(ldUpUtils.getConfiguration().getGroupMemberAttribute());
                members.addStringValues(groupMembersToAdd);
                result.modifications.add(new AttributeModification(AttributeModification.Type.ADD, members));
            }
            if (!groupMembersToRemove.isEmpty()) {
                LdapAttribute members = new LdapAttribute(ldUpUtils.getConfiguration().getGroupMemberAttribute());
                members.addStringValues(groupMembersToRemove);
                result.modifications.add(new AttributeModification(AttributeModification.Type.DELETE, members));
            }
            if (!groupMembersToReplace.isEmpty()) {
                LdapAttribute members = new LdapAttribute(ldUpUtils.getConfiguration().getGroupMemberAttribute());
                members.addStringValues(groupMembersToReplace);
                result.modifications.add(new AttributeModification(AttributeModification.Type.REPLACE, members));
            }
        }

        return result;
    }

    @Override
    public Set<AttributeDelta> updateDelta(
            final ObjectClass objectClass,
            final Uid uid,
            final Set<AttributeDelta> modifications,
            final OperationOptions options) {

        String dn = findDn(objectClass, uid);

        ProcessDeltaResult result = processDelta(objectClass, modifications);

        // 1. update
        if (!result.modifications.isEmpty()) {
            try {
                ModifyOperation.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        throwIf(ResultPredicate.NOT_SUCCESS).
                        build().execute(ModifyRequest.builder().
                                dn(dn).
                                modifications(result.modifications).
                                build());
            } catch (LdapException e) {
                throw new ConnectorException("Update error", e);
            }
        }

        if (ldUpUtils.isAccount(objectClass)) {
            // 2. set password if provided
            setPassword(dn, result.passwordValue);

            // 3. set group memberships if provided
            groupMod(dn, result.groupsToAdd, AttributeModification.Type.ADD);
            groupMod(dn, result.groupsToRemove, AttributeModification.Type.DELETE);
            groupMod(dn, result.groupsToReplace, AttributeModification.Type.REPLACE);
        }

        return modifications;
    }

    @Override
    public Uid addAttributeValues(
            final ObjectClass objectClass,
            final Uid uid,
            final Set<Attribute> valuesToAdd,
            final OperationOptions options) {

        String dn = findDn(objectClass, uid);

        ProcessResult result = process(objectClass, valuesToAdd);

        // 1. update
        if (!result.ldapAttrs.isEmpty()) {
            try {
                ModifyOperation.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        throwIf(ResultPredicate.NOT_SUCCESS).
                        build().execute(ModifyRequest.builder().
                                dn(dn).
                                modifications(result.ldapAttrs.stream().
                                        map(ldapAttr -> new AttributeModification(
                                        AttributeModification.Type.ADD, ldapAttr)).
                                        collect(Collectors.toList())).
                                build());
            } catch (LdapException e) {
                throw new ConnectorException("Update error", e);
            }
        }

        if (ldUpUtils.isAccount(objectClass)) {
            // 2. set password if provided
            setPassword(dn, result.passwordValue);

            // 3. set group memberships if provided
            groupMod(dn, result.groups, AttributeModification.Type.ADD);
        }

        return uid(objectClass, dn);
    }

    @Override
    public Uid removeAttributeValues(
            final ObjectClass objectClass,
            final Uid uid,
            final Set<Attribute> valuesToRemove,
            final OperationOptions options) {

        String dn = findDn(objectClass, uid);

        ProcessResult result = process(objectClass, valuesToRemove);

        // 1. update
        if (!result.ldapAttrs.isEmpty()) {
            try {
                ModifyOperation.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        throwIf(ResultPredicate.NOT_SUCCESS).
                        build().execute(ModifyRequest.builder().
                                dn(dn).
                                modifications(result.ldapAttrs.stream().
                                        map(ldapAttr -> new AttributeModification(
                                        AttributeModification.Type.DELETE, ldapAttr)).
                                        collect(Collectors.toList())).
                                build());
            } catch (LdapException e) {
                throw new ConnectorException("Update error", e);
            }
        }

        if (ldUpUtils.isAccount(objectClass)) {
            // 2. remove password if requested
            if (valuesToRemove.stream().anyMatch(attr -> attr.is(OperationalAttributes.PASSWORD_NAME))) {
                try {
                    ModifyOperation.builder().
                            factory(ldUpUtils.getConnectionFactory()).
                            throwIf(ResultPredicate.NOT_SUCCESS).
                            build().execute(ModifyRequest.builder().
                                    dn(dn).
                                    modifications(new AttributeModification(AttributeModification.Type.DELETE,
                                            new LdapAttribute(ldUpUtils.getConfiguration().getPasswordAttribute()))).
                                    build());
                } catch (LdapException e) {
                    throw new ConnectorException("Remove password error", e);
                }
            }

            // 3. set group memberships if provided
            groupMod(dn, result.groups, AttributeModification.Type.DELETE);
        }

        return uid(objectClass, dn);
    }
}
