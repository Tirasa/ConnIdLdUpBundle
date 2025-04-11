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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.tirasa.connid.bundles.ldup.AbstractLdUpConnectorTests;
import net.tirasa.connid.bundles.ldup.LdUpConfiguration;
import net.tirasa.connid.bundles.ldup.LdUpConstants;
import org.identityconnectors.common.IOUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectReference;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.junit.jupiter.api.Test;
import org.ldaptive.DeleteOperation;
import org.ldaptive.DeleteRequest;
import org.ldaptive.LdapException;

class LdUpCreateOpTests extends AbstractLdUpConnectorTests {

    private static final ObjectClass DEVICE_OBJECT_CLASS = new ObjectClass("device");

    private static void delete(final String dn) throws LdapException {
        DeleteOperation.builder().
                factory(singleConnectionFactory()).
                build().execute(DeleteRequest.builder().
                        dn(dn).
                        build());
    }

    private ConnectorObject doCreateAccount(final ConnectorFacade facade, final OperationOptions options) {
        Name name = new Name("uid=another.worker,ou=People,o=isp");
        Set<Attribute> attributes = Set.of(
                name,
                AttributeBuilder.build("uid", "another.worker"),
                AttributeBuilder.build("cn", "Another Worker"),
                AttributeBuilder.build("givenName", "Another"),
                AttributeBuilder.build("sn", "Worker"));

        Uid uid = facade.create(ObjectClass.ACCOUNT, attributes, options);

        ConnectorObject newAccount = facade.getObject(ObjectClass.ACCOUNT, uid, options);
        assertEquals(name, newAccount.getName());
        return newAccount;
    }

    @Test
    void createAccount() throws LdapException {
        ConnectorFacade facade = newFacade();

        ConnectorObject created = doCreateAccount(facade, null);
        delete(created.getName().getNameValue());
    }

    @Test
    void createAccountWhenUidNotDefault() throws LdapException {
        LdUpConfiguration config = newConfiguration();
        assertFalse(config.getUidAttribute().equalsIgnoreCase("entryDN"));
        config.setUidAttribute("entryDN");
        config.setGidAttribute("entryDN");
        ConnectorFacade facade = newFacade(config);

        ConnectorObject created = doCreateAccount(facade, null);
        delete(created.getName().getNameValue());
    }

    private ConnectorObject doCreateGroup(final ConnectorFacade facade) {
        Name name = new Name("cn=Another Group,ou=Groups,o=isp");
        Set<Attribute> attributes = Set.of(
                name,
                AttributeBuilder.build("cn", "Another Group"),
                AttributeBuilder.build("uniqueMember", "cn=admin,o=isp"));

        Uid uid = facade.create(ObjectClass.GROUP, attributes, null);

        ConnectorObject newGroup = facade.getObject(ObjectClass.GROUP, uid, null);
        assertEquals(name, newGroup.getName());
        return newGroup;
    }

    @Test
    void createGroup() throws LdapException {
        LdUpConfiguration config = newConfiguration();
        config.setLegacyCompatibilityMode(true);
        ConnectorFacade facade = newFacade(config);

        ConnectorObject created = doCreateGroup(facade);
        delete(created.getName().getNameValue());
    }

    @Test
    void createGroupWhenUidNotDefault() throws LdapException {
        LdUpConfiguration config = newConfiguration();
        config.setUidAttribute("entryDN");
        config.setGidAttribute("entryDN");
        config.setLegacyCompatibilityMode(true);
        ConnectorFacade facade = newFacade(config);

        ConnectorObject created = doCreateGroup(facade);
        delete(created.getName().getNameValue());
    }

    private ConnectorObject doCreateDevice(final ConnectorFacade facade) {
        Name name = new Name("cn=laptop,o=isp");
        Set<Attribute> attributes = Set.of(
                name,
                AttributeBuilder.build("cn", "laptop"),
                AttributeBuilder.build("serialNumber", "42"));

        Uid uid = facade.create(DEVICE_OBJECT_CLASS, attributes, null);

        ConnectorObject newObject = facade.getObject(DEVICE_OBJECT_CLASS, uid, null);
        assertEquals(name, newObject.getName());
        return newObject;
    }

    @Test
    void createDeviceWhenNameAttributesNotDefault() throws LdapException {
        LdUpConfiguration config = newConfiguration();
        assertFalse(config.getUidAttribute().equalsIgnoreCase("entryDN"));
        config.setAidAttribute("cn");
        ConnectorFacade facade = newFacade(config);

        ConnectorObject created = doCreateDevice(facade);
        delete(created.getName().getNameValue());
    }

    @Test
    void createDeviceWhenObjectClassesNotDefault() throws LdapException {
        LdUpConfiguration config = newConfiguration();
        assertFalse(config.getUidAttribute().equalsIgnoreCase("entryDN"));
        ConnectorFacade facade = newFacade(config);

        ConnectorObject created = doCreateDevice(facade);
        delete(created.getName().getNameValue());
    }

    @Test
    void createBinaryAttributes() throws IOException, LdapException {
        LdUpConfiguration config = newConfiguration();
        config.setUidAttribute("uid");
        ConnectorFacade facade = newFacade(config);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name("uid=daffy.duck,ou=People,o=isp"));
        attributes.add(AttributeBuilder.build("uid", "daffy.duck"));
        attributes.add(AttributeBuilder.build("cn", "Daffy Duck"));
        attributes.add(AttributeBuilder.build("givenName", "Daffy"));
        attributes.add(AttributeBuilder.build("sn", "Duck"));

        byte[] certificate;
        try (InputStream in = getClass().getResourceAsStream("/certificate.cert")) {
            certificate = IOUtil.inputStreamToBytes(in);
        }
        attributes.add(AttributeBuilder.build("userCertificate;binary", List.of(certificate)));

        byte[] photo;
        try (InputStream in = getClass().getResourceAsStream("/photo.jpg")) {
            photo = IOUtil.inputStreamToBytes(in);
        }
        attributes.add(AttributeBuilder.build("jpegPhoto", List.of(photo)));

        Uid uid = facade.create(ObjectClass.ACCOUNT, attributes, null);

        ConnectorObject newAccount = facade.getObject(ObjectClass.ACCOUNT, uid,
                new OperationOptionsBuilder().setAttributesToGet("userCertificate;binary", "jpegPhoto").build());
        byte[] storedCertificate = (byte[]) newAccount.getAttributeByName("userCertificate;binary").getValue().get(0);
        assertTrue(Arrays.equals(certificate, storedCertificate));
        byte[] storedPhoto = (byte[]) newAccount.getAttributeByName("jpegPhoto").getValue().get(0);
        assertTrue(Arrays.equals(photo, storedPhoto));

        delete(newAccount.getName().getNameValue());
    }

    @Test
    void createPassword() throws LdapException {
        LdUpConfiguration config = newConfiguration();
        config.setUidAttribute("uid");
        ConnectorFacade facade = newFacade(config);

        GuardedString password = new GuardedString("I.hate.rabbits".toCharArray());

        Set<Attribute> attributes = Set.of(
                new Name("uid=daffy.duck,ou=People,o=isp"),
                AttributeBuilder.build("uid", "daffy.duck"),
                AttributeBuilder.build("cn", "Daffy Duck"),
                AttributeBuilder.build("givenName", "Daffy"),
                AttributeBuilder.build("sn", "Duck"),
                AttributeBuilder.buildPassword(password));
        Uid uid = facade.create(ObjectClass.ACCOUNT, attributes, null);

        facade.authenticate(ObjectClass.ACCOUNT, "daffy.duck", password, null);

        delete(facade.getObject(ObjectClass.ACCOUNT, uid, null).getName().getNameValue());
    }

    @Test
    void createWithGroupMembershipLegacy() throws LdapException {
        LdUpConfiguration config = newConfiguration();
        config.setLegacyCompatibilityMode(true);
        ConnectorFacade facade = newFacade(config);

        // 1. create group
        Name name = new Name("cn=Cool Group,ou=Groups,o=isp");
        Set<Attribute> attributes = Set.of(
                name,
                AttributeBuilder.build("cn", "Cool Group"),
                AttributeBuilder.build("uniqueMember", "cn=admin,o=isp"));

        Uid groupUid = facade.create(ObjectClass.GROUP, attributes, null);

        ConnectorObject newGroup = facade.getObject(ObjectClass.GROUP, groupUid,
                new OperationOptionsBuilder().setAttributesToGet("uniqueMember").build());
        assertEquals(1, newGroup.getAttributeByName("uniqueMember").getValue().size());

        // 2. create user
        attributes = Set.of(
                new Name("uid=cool.user,ou=People,o=isp"),
                AttributeBuilder.build("uid", "cool.user"),
                AttributeBuilder.build("cn", "Cool User"),
                AttributeBuilder.build("givenName", "Cool"),
                AttributeBuilder.build("sn", "User"),
                AttributeBuilder.build(LdUpConstants.LEGACY_GROUPS_ATTR_NAME, newGroup.getName().getNameValue()));
        Uid userUid = facade.create(ObjectClass.ACCOUNT, attributes, null);

        ConnectorObject newUser = facade.getObject(ObjectClass.ACCOUNT, userUid,
                new OperationOptionsBuilder().setAttributesToGet(LdUpConstants.LEGACY_GROUPS_ATTR_NAME).build());
        assertEquals(1, newUser.getAttributeByName(LdUpConstants.LEGACY_GROUPS_ATTR_NAME).getValue().size());

        newGroup = facade.getObject(ObjectClass.GROUP, groupUid,
                new OperationOptionsBuilder().setAttributesToGet("uniqueMember").build());
        assertEquals(2, newGroup.getAttributeByName("uniqueMember").getValue().size());
        assertTrue(newGroup.getAttributeByName("uniqueMember").getValue().contains(newUser.getName().getNameValue()));

        // cleanup
        delete(newUser.getName().getNameValue());
        delete(newGroup.getName().getNameValue());
    }

    @Test
    void createWithGroupMembership() throws LdapException {
        ConnectorFacade facade = newFacade();

        // 1. create group
        Name name = new Name("cn=Cool Group,ou=Groups,o=isp");
        Set<Attribute> attributes = Set.of(
                name,
                AttributeBuilder.build("cn", "Cool Group"),
                AttributeBuilder.build(
                        LdUpConstants.MEMBERS_ATTR_NAME,
                        new ConnectorObjectReference(new ConnectorObjectBuilder().
                                setName("cn=admin,o=isp").
                                setObjectClass(ObjectClass.ACCOUNT).
                                buildIdentification())));

        Uid groupUid = facade.create(ObjectClass.GROUP, attributes, null);

        ConnectorObject newGroup = facade.getObject(ObjectClass.GROUP, groupUid,
                new OperationOptionsBuilder().setAttributesToGet(LdUpConstants.MEMBERS_ATTR_NAME).build());
        assertEquals(1, newGroup.getAttributeByName(LdUpConstants.MEMBERS_ATTR_NAME).getValue().size());

        // 2. create user
        attributes = Set.of(
                new Name("uid=cool.user,ou=People,o=isp"),
                AttributeBuilder.build("uid", "cool.user"),
                AttributeBuilder.build("cn", "Cool User"),
                AttributeBuilder.build("givenName", "Cool"),
                AttributeBuilder.build("sn", "User"),
                AttributeBuilder.build(
                        PredefinedAttributes.GROUPS_NAME,
                        new ConnectorObjectReference(new ConnectorObjectBuilder().
                                setName(newGroup.getName().getNameValue()).
                                setObjectClass(ObjectClass.ACCOUNT).
                                buildIdentification())));
        Uid userUid = facade.create(ObjectClass.ACCOUNT, attributes, null);

        ConnectorObject newUser = facade.getObject(ObjectClass.ACCOUNT, userUid,
                new OperationOptionsBuilder().setAttributesToGet(PredefinedAttributes.GROUPS_NAME).build());
        assertEquals(1, newUser.getAttributeByName(PredefinedAttributes.GROUPS_NAME).getValue().size());

        newGroup = facade.getObject(ObjectClass.GROUP, groupUid,
                new OperationOptionsBuilder().setAttributesToGet(LdUpConstants.MEMBERS_ATTR_NAME).build());
        assertEquals(2, newGroup.getAttributeByName(LdUpConstants.MEMBERS_ATTR_NAME).getValue().size());
        assertTrue(newGroup.getAttributeByName(LdUpConstants.MEMBERS_ATTR_NAME).getValue().stream().
                map(ConnectorObjectReference.class::cast).
                anyMatch(v -> newUser.getName().equals(v.getValue().getAttributeByName(Name.NAME))));

        // cleanup
        delete(newUser.getName().getNameValue());
        delete(newGroup.getName().getNameValue());
    }
}
