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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectReference;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.ldaptive.schema.AttributeUsage;
import org.ldaptive.schema.ObjectClassType;
import org.ldaptive.schema.SchemaFactory;

public class LdUpSchemaOp implements SchemaOp {

    protected static final Log LOG = Log.getLog(LdUpSchemaOp.class);

    protected static Optional<AttributeInfo> toAttributeInfo(
            final org.ldaptive.schema.Schema ldapSchema,
            final String attr,
            final Optional<AttributeInfo.Flags> add) {

        return Optional.ofNullable(ldapSchema.getAttributeType(attr)).map(attrType -> {
            Set<AttributeInfo.Flags> flags = EnumSet.noneOf(AttributeInfo.Flags.class);

            if (!attrType.isSingleValued()) {
                flags.add(AttributeInfo.Flags.MULTIVALUED);
            }
            if (attrType.isNoUserModification() || "objectClass".equalsIgnoreCase(attr)) {
                flags.add(AttributeInfo.Flags.NOT_CREATABLE);
                flags.add(AttributeInfo.Flags.NOT_UPDATEABLE);
            }
            if (attrType.getUsage() != AttributeUsage.USER_APPLICATIONS) {
                flags.add(AttributeInfo.Flags.NOT_RETURNED_BY_DEFAULT);
            }

            add.ifPresent(flags::add);

            return AttributeInfoBuilder.build(
                    attr,
                    Arrays.binarySearch(ldapSchema.getBinaryAttributeNames(), attr) < 0 ? String.class : byte[].class,
                    flags);
        });
    }

    protected final LdUpUtils ldUpUtils;

    protected Schema schema;

    public LdUpSchemaOp(final LdUpUtils ldUpUtils) {
        this.ldUpUtils = ldUpUtils;
    }

    @Override
    public Schema schema() {
        synchronized (ldUpUtils) {
            if (schema == null) {
                SchemaBuilder schemaBld = new SchemaBuilder(LdUpConnector.class);

                try {
                    org.ldaptive.schema.Schema ldapSchema =
                            SchemaFactory.createSchema(ldUpUtils.getConnectionFactory());

                    ldapSchema.getObjectClasses().forEach(ldapClass -> {
                        ObjectClassInfoBuilder objClassBld = new ObjectClassInfoBuilder();
                        objClassBld.setType(ldapClass.getName());
                        objClassBld.setAuxiliary(ldapClass.getObjectClassType() == ObjectClassType.AUXILIARY);
                        // Any LDAP object class can contain sub-entries
                        objClassBld.setContainer(ldapClass.getObjectClassType() == ObjectClassType.STRUCTURAL);

                        Set<String> required = Optional.ofNullable(ldapClass.getRequiredAttributes()).
                                map(c -> Stream.of(c).collect(Collectors.toSet())).
                                orElseGet(() -> Set.of());
                        Set<String> optional = Optional.ofNullable(ldapClass.getOptionalAttributes()).
                                map(c -> Stream.of(c).collect(Collectors.toSet())).
                                orElseGet(() -> new HashSet<>());
                        // OpenLDAP's ipProtocol has MUST ( ... $ description ) MAY ( description )
                        optional.removeAll(required);

                        Set<AttributeInfo> attrInfos = new HashSet<>();
                        required.forEach(
                                attr -> toAttributeInfo(ldapSchema, attr, Optional.of(AttributeInfo.Flags.REQUIRED)).
                                        ifPresentOrElse(
                                                attrInfos::add,
                                                () -> LOG.warn("Could not find attribute {0} in object classes {1}",
                                                        attr, ldapClass.getName())));
                        optional.forEach(attr -> toAttributeInfo(ldapSchema, attr, Optional.empty()).
                                ifPresentOrElse(
                                        attrInfos::add,
                                        () -> LOG.warn("Could not find attribute {0} in object classes {1}",
                                                attr, ldapClass.getName())));
                        objClassBld.addAllAttributeInfo(attrInfos);

                        if (ldUpUtils.getConfiguration().getAccountObjectClass().equals(ldapClass.getName())) {
                            if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                                objClassBld.addAttributeInfo(new AttributeInfoBuilder(
                                        LdUpConstants.LEGACY_GROUPS_ATTR_NAME,
                                        String.class).
                                        setMultiValued(true).
                                        setReturnedByDefault(false).
                                        build());
                            } else {
                                objClassBld.addAttributeInfo(new AttributeInfoBuilder(
                                        PredefinedAttributes.GROUPS_NAME,
                                        ConnectorObjectReference.class).
                                        setReferencedObjectClassName(
                                                ldUpUtils.getConfiguration().getGroupObjectClass()).
                                        setRoleInReference(AttributeInfo.RoleInReference.SUBJECT.toString()).
                                        setMultiValued(true).
                                        setReturnedByDefault(false).
                                        build());
                            }
                        } else if (ldUpUtils.getConfiguration().getGroupObjectClass().equals(ldapClass.getName())) {
                            if (ldUpUtils.getConfiguration().isLegacyCompatibilityMode()) {
                                objClassBld.addAttributeInfo(new AttributeInfoBuilder(
                                        ldUpUtils.getConfiguration().getGroupMemberAttribute(),
                                        String.class).
                                        setMultiValued(true).
                                        setReturnedByDefault(false).
                                        build());
                            } else {
                                objClassBld.addAttributeInfo(new AttributeInfoBuilder(
                                        LdUpConstants.MEMBERS_ATTR_NAME,
                                        ConnectorObjectReference.class).
                                        setReferencedObjectClassName(
                                                ldUpUtils.getConfiguration().getAccountObjectClass()).
                                        setRoleInReference(AttributeInfo.RoleInReference.OBJECT.toString()).
                                        setMultiValued(true).
                                        setReturnedByDefault(false).
                                        build());
                            }
                        }

                        schemaBld.defineObjectClass(objClassBld.build());

                        if (ldUpUtils.getConfiguration().getAccountObjectClass().equals(ldapClass.getName())) {
                            objClassBld.setType(ObjectClass.ACCOUNT_NAME);
                            schemaBld.defineObjectClass(objClassBld.build());
                        } else if (ldUpUtils.getConfiguration().getGroupObjectClass().equals(ldapClass.getName())) {
                            objClassBld.setType(ObjectClass.GROUP_NAME);
                            schemaBld.defineObjectClass(objClassBld.build());
                        }
                    });
                } catch (Exception e) {
                    LOG.error(e, "While building schema");
                }

                schema = schemaBld.build();
            }
        }

        return schema;
    }
}
