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

import java.util.List;
import java.util.Optional;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.test.common.TestHelpers;
import org.ldaptive.BindConnectionInitializer;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.LdapException;
import org.ldaptive.SingleConnectionFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class AbstractLdUpConnectorTests {

    protected static final String INET_ORG_PERSON_CLASS = "inetOrgPerson";

    protected static final String GROUP_OF_UNIQUE_NAMES_CLASS = "groupOfUniqueNames";

    protected static final String TEST_GROUP_DN = "cn=Group1,ou=Groups,o=isp";

    protected static final String USER01_DN = "cn=user01,ou=People,o=isp";

    protected static final String USER01_CN = "user01";

    protected static final String[] ACCOUNT_ATTRS_TO_GET = {
        Uid.NAME, Name.NAME, OperationalAttributes.PASSWORD_NAME, "uid", "cn", "sn", "givenName", "mail",
        PredefinedAttributes.GROUPS_NAME, LdUpConstants.SYNCREPL_COOKIE_NAME };

    protected static final String[] GROUP_ATTRS_TO_GET = {
        Uid.NAME, Name.NAME, "cn", LdUpConstants.MEMBERS_ATTR_NAME, LdUpConstants.SYNCREPL_COOKIE_NAME };

    @Container
    protected static GenericContainer<?> LDAP_CONTAINER = new GenericContainer<>(
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
        return newFacade(newConfiguration());
    }

    protected static ConnectorFacade newFacade(final LdUpConfiguration configuration) {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        APIConfiguration impl = TestHelpers.createTestConfiguration(LdUpConnector.class, configuration);
        impl.getResultsHandlerConfiguration().setFilteredResultsHandlerInValidationMode(true);
        return factory.newInstance(impl);
    }

    protected static SingleConnectionFactory singleConnectionFactory() throws LdapException {
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
        return cf;
    }

    protected static Optional<ConnectorObject> searchByAttribute(
            final ConnectorFacade facade, final ObjectClass oclass, final Attribute attr) {

        return searchByAttribute(facade, oclass, attr, (OperationOptions) null);
    }

    protected static Optional<ConnectorObject> searchByAttribute(
            final ConnectorFacade facade,
            final ObjectClass oclass,
            final Attribute attr,
            final String... attributesToGet) {

        return searchByAttribute(
                facade, oclass, attr, new OperationOptionsBuilder().setAttributesToGet(attributesToGet).build());
    }

    protected static Optional<ConnectorObject> searchByAttribute(
            final ConnectorFacade facade,
            final ObjectClass oclass,
            final Attribute attr,
            final OperationOptions options) {

        List<ConnectorObject> objects = TestHelpers.searchToList(facade, oclass, FilterBuilder.equalTo(attr), options);
        return objects.stream().findFirst();
    }
}
