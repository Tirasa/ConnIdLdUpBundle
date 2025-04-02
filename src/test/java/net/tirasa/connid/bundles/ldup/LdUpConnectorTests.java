/**
 * Copyright (C) 2024 ConnId (connid-dev@googlegroups.com)
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.AssertionsKt.assertNotNull;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfo.Flags;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectReference;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.Test;
import org.ldaptive.AddOperation;
import org.ldaptive.AddRequest;
import org.ldaptive.AttributeModification;
import org.ldaptive.BindConnectionInitializer;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.DefaultConnectionFactory;
import org.ldaptive.DeleteOperation;
import org.ldaptive.DeleteRequest;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapException;
import org.ldaptive.ModifyOperation;
import org.ldaptive.ModifyRequest;
import org.ldaptive.SingleConnectionFactory;
import org.ldaptive.extended.ExtendedOperation;
import org.ldaptive.extended.PasswordModifyRequest;
import org.ldaptive.handler.ResultPredicate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class LdUpConnectorTests {

    private static final String INET_ORG_PERSON_CLASS = "inetOrgPerson";

    private static final String GROUP_OF_UNIQUE_NAMES_CLASS = "groupOfUniqueNames";

    private static final String TEST_GROUP_DN = "cn=Group1,ou=Groups,o=isp";

    private static final String[] ACCOUNT_ATTRS_TO_GET = {
        Uid.NAME, Name.NAME, OperationalAttributes.PASSWORD_NAME, "uid", "cn", "sn", "givenName", "mail",
        PredefinedAttributes.GROUPS_NAME, LdUpConnector.SYNCREPL_COOKIE_NAME };

    private static final String[] GROUP_ATTRS_TO_GET = {
        Uid.NAME, Name.NAME, "cn", LdUpConnector.MEMBERS_ATTR_NAME, LdUpConnector.SYNCREPL_COOKIE_NAME };

    @Container
    static GenericContainer<?> LDAP_CONTAINER = new GenericContainer<>(
            DockerImageName.parse("bitnami/openldap:2.6")).
            waitingFor(Wait.forLogMessage(".*slapd starting.*", 1)).
            withEnv("LDAP_ROOT", "o=isp").
            withEnv("LDAP_USER_OU", "People").
            withEnv("LDAP_GROUP_OU", "Groups").
            withEnv("LDAP_ENABLE_SYNCPROV", "yes");

    protected static LdUpConfiguration newConfiguration() {
        LdUpConfiguration config = new LdUpConfiguration();
        config.setUrl("ldap://" + LDAP_CONTAINER.getContainerInfo().getNetworkSettings().getNetworks().values().
                iterator().next().getIpAddress() + ":1389");
        config.setBindDn("cn=admin,o=isp");
        config.setBindPassword(new GuardedString("adminpassword".toCharArray()));
        config.setBaseDn("o=isp");
        return config;
    }

    protected static ConnectorFacade newFacade() {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        APIConfiguration impl = TestHelpers.createTestConfiguration(LdUpConnector.class, newConfiguration());
        impl.getResultsHandlerConfiguration().setFilteredResultsHandlerInValidationMode(true);
        return factory.newInstance(impl);
    }

    @Test
    void test() {
        newFacade().test();
    }

    @Test
    void schema() {
        Schema schema = newFacade().schema();
        assertNotNull(schema);

        assertTrue(schema.getOperationOptionInfo().isEmpty());
        assertFalse(schema.getObjectClassInfo().isEmpty());

        ObjectClassInfo inetOrgPerson = schema.getObjectClassInfo().stream().
                filter(oci -> INET_ORG_PERSON_CLASS.equalsIgnoreCase(oci.getType())).findFirst().orElseThrow();
        assertFalse(inetOrgPerson.isAuxiliary());
        assertFalse(inetOrgPerson.isEmbedded());
        assertTrue(inetOrgPerson.isContainer());

        assertFalse(inetOrgPerson.getAttributeInfo().isEmpty());

        AttributeInfo uid = inetOrgPerson.getAttributeInfo().stream().
                filter(ai -> "uid".equalsIgnoreCase(ai.getName())).findFirst().orElseThrow();
        assertEquals(String.class, uid.getType());
        assertEquals(EnumSet.of(Flags.MULTIVALUED), uid.getFlags());

        assertTrue(inetOrgPerson.getAttributeInfo().stream().
                filter(ai -> PredefinedAttributes.GROUPS_NAME.equalsIgnoreCase(ai.getName())).findFirst().isPresent());

        ObjectClassInfo account = schema.getObjectClassInfo().stream().
                filter(oci -> ObjectClass.ACCOUNT_NAME.equals(oci.getType())).findFirst().orElseThrow();
        assertEquals(inetOrgPerson.getAttributeInfo(), account.getAttributeInfo());

    }

    private static String createUser(final DefaultConnectionFactory cf) throws LdapException {
        String uid = "user" + UUID.randomUUID().toString().substring(0, 8);
        String userDn = "uid=" + uid + ",ou=People,o=isp";

        AddOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(AddRequest.builder().
                        dn(userDn).
                        attributes(
                                new LdapAttribute("objectClass", INET_ORG_PERSON_CLASS),
                                new LdapAttribute("uid", uid),
                                new LdapAttribute("sn", "Doe"),
                                new LdapAttribute("givenName", "John"),
                                new LdapAttribute("cn", "John Doe"),
                                new LdapAttribute("mail", uid + "@connid.tirasa.net")).
                        build());

        ExtendedOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(new PasswordModifyRequest(userDn, null, "Password123"));

        return userDn;
    }

    private static void updateUser(final DefaultConnectionFactory cf, final String userDn) throws LdapException {
        ModifyOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(ModifyRequest.builder().
                        dn(userDn).
                        modifications(
                                new AttributeModification(AttributeModification.Type.ADD,
                                        new LdapAttribute("telephoneNumber", "+39085000000")),
                                new AttributeModification(AttributeModification.Type.REPLACE,
                                        new LdapAttribute("givenName", "Jane"))).
                        build());

        ModifyOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(ModifyRequest.builder().
                        dn(TEST_GROUP_DN).
                        modifications(
                                new AttributeModification(AttributeModification.Type.ADD,
                                        new LdapAttribute("uniqueMember", userDn))).
                        build());
    }

    private static void deleteUser(final DefaultConnectionFactory cf, final String userDn) throws LdapException {
        DeleteOperation.builder().
                factory(cf).
                build().execute(DeleteRequest.builder().
                        dn(userDn).
                        build());

        ModifyOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(ModifyRequest.builder().
                        dn(TEST_GROUP_DN).
                        modifications(
                                new AttributeModification(AttributeModification.Type.DELETE,
                                        new LdapAttribute("uniqueMember", userDn))).
                        build());
    }

    private static void doLiveSync(
            final ConnectorFacade connector,
            final String objectClass,
            final List<ConnectorObject> processed,
            final String cookie,
            final String... attrsToGet) {

        OperationOptionsBuilder oob = new OperationOptionsBuilder();
        Optional.ofNullable(cookie).ifPresent(oob::setPagedResultsCookie);
        Optional.ofNullable(attrsToGet).ifPresent(oob::setAttributesToGet);

        connector.livesync(
                new ObjectClass(objectClass),
                delta -> {
                    processed.add(delta.getObject());
                    return true;
                },
                oob.build());
    }

    @Test
    void livesync() throws LdapException {
        LdUpConfiguration conf = newConfiguration();

        ConnectionConfig connectionConfig = ConnectionConfig.builder().
                url(conf.getUrl()).
                connectionInitializers(BindConnectionInitializer.builder().
                        dn(conf.getBindDn()).
                        credential(SecurityUtil.decrypt(conf.getBindPassword())).
                        build()).
                build();
        SingleConnectionFactory cf = SingleConnectionFactory.builder().config(connectionConfig).build();
        cf.initialize();

        String jdoe = "uid=jdoe,ou=People,o=isp";
        AddOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(AddRequest.builder().
                        dn(jdoe).
                        attributes(
                                new LdapAttribute("objectClass", INET_ORG_PERSON_CLASS),
                                new LdapAttribute("uid", "jdoe"),
                                new LdapAttribute("sn", "Doe"),
                                new LdapAttribute("givenName", "John"),
                                new LdapAttribute("cn", "John Doe"),
                                new LdapAttribute("mail", "john.doe@connid.tirasa.net")).
                        build());

        AddOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(AddRequest.builder().
                        dn(TEST_GROUP_DN).
                        attributes(
                                new LdapAttribute("objectClass", GROUP_OF_UNIQUE_NAMES_CLASS),
                                new LdapAttribute("cn", "Group1"),
                                new LdapAttribute("uniqueMember", jdoe)).
                        build());

        ConnectorFacade connector = newFacade();
        List<ConnectorObject> users = new ArrayList<>();
        doLiveSync(connector, INET_ORG_PERSON_CLASS, users, null, ACCOUNT_ATTRS_TO_GET);

        // 2 predefined users in the Docker image + 1 as created above
        assertEquals(3, users.size());

        assertTrue(users.stream().
                allMatch(object -> INET_ORG_PERSON_CLASS.equals(object.getObjectClass().getObjectClassValue())));
        assertTrue(users.stream().allMatch(o -> o.getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME) != null));
        assertTrue(users.stream().allMatch(o -> o.getAttributeByName("uid") != null));
        assertTrue(users.stream().allMatch(o -> o.getAttributeByName("sn") != null));
        assertTrue(users.stream().anyMatch(o -> AttributeUtil.getPasswordValue(o.getAttributes()) != null));
        assertTrue(users.stream().anyMatch(o -> o.getAttributeByName("givenName") != null));
        assertTrue(users.stream().anyMatch(o -> o.getAttributeByName("mail") != null));
        assertTrue(users.stream().anyMatch(o -> o.getAttributeByName(PredefinedAttributes.GROUPS_NAME) != null));

        String cookie = AttributeUtil.getStringValue(
                users.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME));
        assertNotNull(cookie);
        assertTrue(users.stream().
                allMatch(object -> users.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME).
                equals(object.getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME))));

        // 1. create user
        String userDn = createUser(cf);

        users.clear();
        doLiveSync(connector, INET_ORG_PERSON_CLASS, users, cookie, ACCOUNT_ATTRS_TO_GET);

        assertEquals(1, users.size());

        String entryUUID = users.get(0).getUid().getUidValue();
        assertNotNull(entryUUID);

        assertEquals(userDn, users.get(0).getName().getNameValue());

        assertTrue(INET_ORG_PERSON_CLASS.equals(users.get(0).getObjectClass().getObjectClassValue()));

        assertTrue(users.get(0).getAttributeByName(PredefinedAttributes.GROUPS_NAME).getValue().isEmpty());

        cookie = AttributeUtil.getStringValue(users.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME));
        assertNotNull(cookie);

        // 2. update user
        updateUser(cf, userDn);

        // 2a. live sync users
        users.clear();
        doLiveSync(connector, INET_ORG_PERSON_CLASS, users, cookie, ACCOUNT_ATTRS_TO_GET);

        assertEquals(1, users.size());

        assertEquals(entryUUID, users.get(0).getUid().getUidValue());
        assertEquals(userDn, users.get(0).getName().getNameValue());

        assertTrue(INET_ORG_PERSON_CLASS.equals(users.get(0).getObjectClass().getObjectClassValue()));

        Attribute userGroups = users.get(0).getAttributeByName(PredefinedAttributes.GROUPS_NAME);
        assertNotNull(userGroups);
        assertEquals(1, userGroups.getValue().size());
        assertTrue(userGroups.getValue().get(0) instanceof ConnectorObjectReference);
        ConnectorObjectReference userGroup = (ConnectorObjectReference) userGroups.getValue().get(0);
        assertEquals(GROUP_OF_UNIQUE_NAMES_CLASS, userGroup.getValue().getObjectClass().getObjectClassValue());
        assertEquals(TEST_GROUP_DN, userGroup.getValue().getAttributeByName(Name.NAME).getValue().get(0));

        // 2b. live sync groups
        List<ConnectorObject> groups = new ArrayList<>();
        doLiveSync(connector, GROUP_OF_UNIQUE_NAMES_CLASS, groups, cookie, GROUP_ATTRS_TO_GET);

        assertEquals(1, groups.size());

        assertEquals(TEST_GROUP_DN, groups.get(0).getName().getNameValue());

        Attribute groupMembers = groups.get(0).getAttributeByName(LdUpConnector.MEMBERS_ATTR_NAME);
        assertNotNull(groupMembers);
        assertEquals(2, groupMembers.getValue().size());
        assertTrue(groupMembers.getValue().stream().allMatch(ConnectorObjectReference.class::isInstance));
        assertTrue(groupMembers.getValue().stream().map(ConnectorObjectReference.class::cast).
                allMatch(m -> INET_ORG_PERSON_CLASS.equals(m.getValue().getObjectClass().getObjectClassValue())));
        assertTrue(groupMembers.getValue().stream().map(ConnectorObjectReference.class::cast).
                anyMatch(m -> jdoe.equals(m.getValue().getAttributeByName(Name.NAME).getValue().get(0))));
        assertTrue(groupMembers.getValue().stream().map(ConnectorObjectReference.class::cast).
                anyMatch(m -> userDn.equals(m.getValue().getAttributeByName(Name.NAME).getValue().get(0))));

        cookie = AttributeUtil.getStringValue(groups.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME));
        assertNotNull(cookie);

        // 3. delete user
        deleteUser(cf, userDn);

        users.clear();
        doLiveSync(connector, INET_ORG_PERSON_CLASS, users, cookie);

        assertEquals(1, users.size());

        assertEquals(entryUUID, users.get(0).getUid().getUidValue());
        assertEquals(entryUUID, users.get(0).getName().getNameValue());

        cookie = AttributeUtil.getStringValue(users.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME));
        assertNotNull(cookie);
    }

    @Test
    void getLatestSyncToken() {
        assertNotNull(newFacade().getLatestSyncToken(new ObjectClass(GROUP_OF_UNIQUE_NAMES_CLASS)));
    }

    @Test
    void sync() {
        newFacade().sync(new ObjectClass(GROUP_OF_UNIQUE_NAMES_CLASS),
                null,
                delta -> {
                    assertNotNull(delta.getToken());
                    assertNotNull(delta.getObject());
                    return true;
                },
                new OperationOptionsBuilder().setAttributesToGet(ACCOUNT_ATTRS_TO_GET).build());
    }
}
