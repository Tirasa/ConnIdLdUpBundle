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
package net.tirasa.connid.bundles.ldup.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import net.tirasa.connid.bundles.ldup.AbstractLdUpConnectorTests;
import net.tirasa.connid.bundles.ldup.LdUpConfiguration;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfoUtil;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.SearchResult;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.SearchResultsHandler;
import org.identityconnectors.test.common.ToListResultsHandler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.ldaptive.AttributeModification;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapException;
import org.ldaptive.ModifyOperation;
import org.ldaptive.ModifyRequest;
import org.ldaptive.SingleConnectionFactory;
import org.ldaptive.handler.ResultPredicate;

class LdUpSearchOpTests extends AbstractLdUpConnectorTests {

    private static final String USER02_DN = "cn=user01,ou=People,o=isp";

    private static void searchExpectingNoResult(
            final LdUpConfiguration config, final LdUpFilter filter, final OperationOptions options) {

        LdUpSearchOp searchOp = new LdUpSearchOp(new LdUpUtils(config));
        ToListResultsHandler handler = new ToListResultsHandler();
        searchOp.executeQuery(ObjectClass.ACCOUNT, filter, handler, options);
        assertTrue(handler.getObjects().isEmpty());
    }

    @Test
    void ldapFilter() {
        LdUpSearchOp searchOp = new LdUpSearchOp(new LdUpUtils(newConfiguration()));

        LdUpFilter filter = LdUpFilter.forEntryDN(USER01_DN);
        ToListResultsHandler handler = new ToListResultsHandler();
        searchOp.executeQuery(ObjectClass.ACCOUNT, filter, handler, new OperationOptionsBuilder().build());
        assertEquals(1, handler.getObjects().size());

        filter = LdUpFilter.forNativeFilter("foo=bar");
        handler = new ToListResultsHandler();
        searchOp.executeQuery(ObjectClass.ACCOUNT, filter, handler, new OperationOptionsBuilder().build());
        assertTrue(handler.getObjects().isEmpty());
    }

    @Test
    void ldapFilterWithNonExistingEntryDN() {
        LdUpFilter filter = LdUpFilter.forEntryDN("dc=foo,dc=bar");

        // Simple paged results.
        LdUpConfiguration config = newConfiguration();
        searchExpectingNoResult(config, filter, new OperationOptionsBuilder().setPageSize(25).build());

        // No paging.
        config = newConfiguration();
        searchExpectingNoResult(config, filter, new OperationOptionsBuilder().build());
    }

    @Test
    void ldapFilterWithInvalidEntryDN() {
        LdUpFilter filter = LdUpFilter.forEntryDN("dc=foo,,");

        // Simple paged results.
        LdUpConfiguration config = newConfiguration();
        searchExpectingNoResult(config, filter, new OperationOptionsBuilder().setPageSize(25).build());

        // No paging.
        config = newConfiguration();
        searchExpectingNoResult(config, filter, new OperationOptionsBuilder().build());
    }

    @Test
    void getObject() {
        LdUpConfiguration config = newConfiguration();
        config.setUidAttribute("uid");
        assertNotNull(newFacade(config).getObject(ObjectClass.ACCOUNT, new Uid(USER01_CN), null));
    }

    @Test
    void simplePagedSearch() {
        LdUpConfiguration config = newConfiguration();
        ConnectorFacade facade = newFacade(config);

        AtomicReference<String> cookie = new AtomicReference<>();
        List<ConnectorObject> objects = new ArrayList<>();
        facade.search(
                ObjectClass.ACCOUNT,
                null,
                new SearchResultsHandler() {

            @Override
            public void handleResult(final SearchResult result) {
                cookie.set(result.getPagedResultsCookie());
            }

            @Override
            public boolean handle(final ConnectorObject connectorObject) {
                objects.add(connectorObject);
                return true;
            }
        }, new OperationOptionsBuilder().setPageSize(1).build());

        assertNotNull(cookie.get());
        assertEquals(1, objects.size());
    }

    @Test
    void withFilter() {
        ConnectorFacade facade = newFacade();
        ConnectorObject bunny = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();
        assertEquals(USER01_DN, bunny.getName().getNameValue());
    }

    @Disabled("OpenLDAP does not seem to work with binary attribute filter")
    @Test
    void withFilterByBinaryAttribute() throws LdapException {
        ConnectorFacade facade = newFacade();
        ConnectorObject bunny = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        byte[] photo = { -4, -3, -2, -1, 0, 1, 2, 3, 63, 127 };

        SingleConnectionFactory cf = singleConnectionFactory();
        ModifyOperation.builder().
                factory(cf).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(ModifyRequest.builder().
                        dn(USER01_DN).
                        modifications(
                                new AttributeModification(AttributeModification.Type.ADD,
                                        new LdapAttribute("jpegPhoto", photo))).
                        build());
        Attribute photoAttr = AttributeBuilder.build("jpegPhoto", Set.of(photo));
        ConnectorObject bunnyWithPhoto = searchByAttribute(facade, ObjectClass.ACCOUNT, photoAttr, "jpegPhoto").
                orElseThrow();
        assertEquals(bunny.getUid(), bunnyWithPhoto.getUid());
    }

    @Test
    void attributesToGet() {
        ConnectorFacade facade = newFacade();
        ConnectorObject object = searchByAttribute(
                facade, ObjectClass.ACCOUNT, new Name(USER02_DN), "employeeNumber", "telephoneNumber").orElseThrow();

        Set<Attribute> attrs = new HashSet<>(object.getAttributes());
        assertTrue(attrs.remove(AttributeUtil.find(Uid.NAME, attrs)));
        assertTrue(attrs.remove(AttributeUtil.find(Name.NAME, attrs)));
        assertTrue(attrs.remove(AttributeUtil.find("employeeNumber", attrs)));
        assertTrue(attrs.remove(AttributeUtil.find("telephoneNumber", attrs)));

        assertTrue(attrs.isEmpty());
    }

    @Test
    void attributesReturnedByDefaultWithNoValueAreNotReturned() {
        LdUpConfiguration config = newConfiguration();
        ConnectorFacade facade = newFacade(config);
        AttributeInfo attr = AttributeInfoUtil.find("givenName", facade.schema().
                findObjectClassInfo(ObjectClass.ACCOUNT_NAME).getAttributeInfo());
        assertTrue(attr.isReturnedByDefault());

        ConnectorObject object = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();
        assertNull(object.getAttributeByName("givenName"));
    }

    @Test
    void attributesToGetNotPresentInEntryAreEmpty() {
        ConnectorFacade facade = newFacade();
        ConnectorObject object = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN), "employeeNumber").
                orElseThrow();

        assertTrue(object.getAttributeByName("employeeNumber").getValue().isEmpty());
    }

    @Test
    void gidAttributeCn() {
        LdUpConfiguration config = newConfiguration();
        assertFalse(config.getGidAttribute().equalsIgnoreCase("cn"));
        config.setGroupObjectClass("groupOfNames");
        config.setGidAttribute("cn");
        ConnectorFacade facade = newFacade(config);

        ConnectorObject readers = searchByAttribute(facade, ObjectClass.GROUP, new Uid("readers")).orElseThrow();
        assertEquals("readers", readers.getUid().getUidValue());
        assertEquals("cn=readers,ou=Groups,o=isp", readers.getName().getNameValue());
    }

    @Test
    void uidAttributeEntryDN() {
        LdUpConfiguration config = newConfiguration();
        assertFalse(config.getUidAttribute().equalsIgnoreCase("entryDN"));
        config.setUidAttribute("entryDN");
        config.setGidAttribute("entryDN");
        ConnectorFacade facade = newFacade(config);

        ConnectorObject bunny = searchByAttribute(facade, ObjectClass.ACCOUNT, new Uid(USER01_DN)).orElseThrow();
        assertEquals(USER01_DN, bunny.getName().getNameValue());
    }
}
