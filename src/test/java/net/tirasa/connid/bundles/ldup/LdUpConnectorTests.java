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
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfo.Flags;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
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
import org.ldaptive.DeleteResponse;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapException;
import org.ldaptive.ModifyOperation;
import org.ldaptive.ModifyRequest;
import org.ldaptive.SingleConnectionFactory;
import org.ldaptive.auth.SearchDnResolver;
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

        SearchDnResolver dnResolver = SearchDnResolver.builder().
                factory(cf).
                dn("ou=People,o=isp").
                filter("(uid={user})").
                build();

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
                        dn("cn=Group1,ou=Groups,o=isp").
                        modifications(
                                new AttributeModification(AttributeModification.Type.ADD,
                                        new LdapAttribute("uniqueMember", userDn))).
                        build());
    }

    private static void deleteUser(final DefaultConnectionFactory cf, final String userDn) throws LdapException {
        DeleteResponse response = new DeleteOperation(cf).execute(new DeleteRequest(userDn));
        assertTrue(response.isSuccess());
    }

    private static void doLiveSync(
            final ConnectorFacade connector,
            final List<ConnectorObject> processed,
            final String cookie,
            final String... attrsToGet) {

        OperationOptionsBuilder oob = new OperationOptionsBuilder();
        Optional.ofNullable(cookie).ifPresent(oob::setPagedResultsCookie);
        Optional.ofNullable(attrsToGet).ifPresent(oob::setAttributesToGet);

        connector.livesync(
                new ObjectClass(INET_ORG_PERSON_CLASS),
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

        String userDn = "uid=jdoe,ou=People,o=isp";
        AddOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(AddRequest.builder().
                        dn(userDn).
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
                        dn("cn=Group1,ou=Groups,o=isp").
                        attributes(
                                new LdapAttribute("objectClass", GROUP_OF_UNIQUE_NAMES_CLASS),
                                new LdapAttribute("cn", "Group1"),
                                new LdapAttribute("uniqueMember", userDn)).
                        build());

        ConnectorFacade connector = newFacade();
        List<ConnectorObject> processed = new ArrayList<>();
        doLiveSync(connector, processed, null,
                Uid.NAME, Name.NAME, OperationalAttributes.PASSWORD_NAME,
                "uid", "sn", "givenName", "mail", LdUpConnector.SYNCREPL_COOKIE_NAME);

        // 2 predefined users in the Docker image + 1 as crated above
        assertEquals(3, processed.size());

        assertTrue(processed.stream().
                allMatch(object -> INET_ORG_PERSON_CLASS.equals(object.getObjectClass().getObjectClassValue())));
        assertTrue(processed.stream().allMatch(o -> o.getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME) != null));
        assertTrue(processed.stream().allMatch(o -> o.getAttributeByName("uid") != null));
        assertTrue(processed.stream().allMatch(o -> o.getAttributeByName("sn") != null));
        assertTrue(processed.stream().anyMatch(o -> o.getAttributeByName("givenName") != null));
        assertTrue(processed.stream().anyMatch(o -> o.getAttributeByName("mail") != null));

        String cookie = AttributeUtil.getStringValue(
                processed.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME));
        assertNotNull(cookie);
        assertTrue(processed.stream().
                allMatch(object -> processed.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME).
                equals(object.getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME))));

        // 1. create user
        userDn = createUser(cf);

        processed.clear();
        doLiveSync(connector, processed, cookie);

        assertEquals(1, processed.size());

        String entryUUID = processed.get(0).getUid().getUidValue();
        assertNotNull(entryUUID);

        assertEquals(userDn, processed.get(0).getName().getNameValue());

        assertTrue(processed.stream().
                allMatch(object -> INET_ORG_PERSON_CLASS.equals(object.getObjectClass().getObjectClassValue())));

        cookie = AttributeUtil.getStringValue(processed.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME));
        assertNotNull(cookie);

        // 2. update user
        updateUser(cf, userDn);

        processed.clear();
        doLiveSync(connector, processed, cookie);

        assertEquals(1, processed.size());

        assertEquals(entryUUID, processed.get(0).getUid().getUidValue());
        assertEquals(userDn, processed.get(0).getName().getNameValue());

        assertTrue(processed.stream().
                allMatch(object -> INET_ORG_PERSON_CLASS.equals(object.getObjectClass().getObjectClassValue())));

        cookie = AttributeUtil.getStringValue(processed.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME));
        assertNotNull(cookie);

        // 3. delete user
        deleteUser(cf, userDn);

        processed.clear();
        doLiveSync(connector, processed, cookie);

        assertEquals(1, processed.size());

        assertEquals(entryUUID, processed.get(0).getUid().getUidValue());
        assertEquals(entryUUID, processed.get(0).getName().getNameValue());

        cookie = AttributeUtil.getStringValue(processed.get(0).getAttributeByName(LdUpConnector.SYNCREPL_COOKIE_NAME));
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
                new OperationOptionsBuilder().build());
    }
}
