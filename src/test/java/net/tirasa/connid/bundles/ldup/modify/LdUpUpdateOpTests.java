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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.tirasa.connid.bundles.ldup.AbstractLdUpConnectorTests;
import net.tirasa.connid.bundles.ldup.LdUpConfiguration;
import org.identityconnectors.common.IOUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeDelta;
import org.identityconnectors.framework.common.objects.AttributeDeltaBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.Uid;
import org.junit.jupiter.api.Test;

class LdUpUpdateOpTests extends AbstractLdUpConnectorTests {

    private static final String DAFFY_DUCK_DN = "uid=daffy.duck,ou=People,o=isp";

    private static final String RENAME_ONE_TEST_DN = "uid=rename.one,ou=People,o=isp";

    private static final String RENAME_TWO_TEST_DN = "uid=rename.two,ou=People,o=isp";

    private static final String NUMBER1 = "+1 800 123 4567";

    private static final String NUMBER2 = "+1 800 765 4321";

    private static final String NUMBER3 = "+1 800 765 9876";

    @Test
    void update() {
        ConnectorFacade facade = newFacade();
        ConnectorObject user = searchByAttribute(
                facade, ObjectClass.ACCOUNT, new Name("cn=user02,ou=People,o=isp"),
                new OperationOptionsBuilder().setAttributesToGet("sn").build()).
                orElseThrow();

        String snBefore = AttributeUtil.getStringValue(user.getAttributeByName("sn"));
        assertNotNull(snBefore);
        assertNotEquals("ConnId", snBefore);

        Set<Attribute> replaceAttrs = new HashSet<>(user.getAttributes());
        replaceAttrs.removeIf(attr -> Uid.NAME.equals(attr.getName()));
        replaceAttrs.removeIf(attr -> Name.NAME.equals(attr.getName()));
        replaceAttrs.removeIf(attr -> "sn".equals(attr.getName()));
        replaceAttrs.add(AttributeBuilder.build("sn", "ConnId"));
        replaceAttrs.add(AttributeBuilder.build("telephoneNumber", NUMBER1));

        facade.update(ObjectClass.ACCOUNT, user.getUid(), replaceAttrs, null);

        ConnectorObject updated = facade.getObject(ObjectClass.ACCOUNT, user.getUid(),
                new OperationOptionsBuilder().setAttributesToGet("sn", "telephoneNumber").build());

        String snAfter = AttributeUtil.getStringValue(updated.getAttributeByName("sn"));
        assertNotNull(snAfter);
        assertEquals("ConnId", snAfter);
        assertEquals(NUMBER1, AttributeUtil.getStringValue(updated.getAttributeByName("telephoneNumber")));
    }

    @Test
    void updateDelta() {
        // 1. take user and set attribute
        LdUpConfiguration config = newConfiguration();
        config.setUidAttribute("cn");
        ConnectorFacade facade = newFacade(config);
        ConnectorObject bugs = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        facade.update(
                ObjectClass.ACCOUNT,
                bugs.getUid(),
                Set.of(
                        AttributeBuilder.build("telephoneNumber", NUMBER1),
                        AttributeBuilder.buildPassword("carrot".toCharArray())),
                null);

        OperationOptions options = new OperationOptionsBuilder().setAttributesToGet("telephoneNumber").build();

        bugs = facade.getObject(ObjectClass.ACCOUNT, bugs.getUid(), options);
        Attribute telephoneAttr = bugs.getAttributeByName("telephoneNumber");
        List<Object> numberAttr = telephoneAttr.getValue();
        assertEquals(1, numberAttr.size());
        assertEquals(NUMBER1, numberAttr.get(0));

        // 2. updateDelta with values to add and to remove
        AttributeDelta delta = AttributeDeltaBuilder.build(
                "telephoneNumber", List.of(NUMBER2), List.of(NUMBER1));
        facade.updateDelta(ObjectClass.ACCOUNT, bugs.getUid(), Set.of(delta), null);

        bugs = facade.getObject(ObjectClass.ACCOUNT, bugs.getUid(), options);
        numberAttr = bugs.getAttributeByName("telephoneNumber").getValue();
        assertEquals(1, numberAttr.size());
        assertEquals(NUMBER2, numberAttr.get(0));

        // 3. updateDelta with values to add
        delta = AttributeDeltaBuilder.build(
                "telephoneNumber", List.of(NUMBER1), List.of());
        facade.updateDelta(ObjectClass.ACCOUNT, bugs.getUid(), Set.of(delta), null);

        bugs = facade.getObject(ObjectClass.ACCOUNT, bugs.getUid(), options);
        numberAttr = bugs.getAttributeByName("telephoneNumber").getValue();
        assertEquals(2, numberAttr.size());
        assertTrue(numberAttr.contains(NUMBER1));
        assertTrue(numberAttr.contains(NUMBER2));

        // 4. updateDelta with values to replace
        assertDoesNotThrow(() -> facade.authenticate(
                ObjectClass.ACCOUNT, USER01_CN, new GuardedString("carrot".toCharArray()), null));

        delta = AttributeDeltaBuilder.build("telephoneNumber", List.of(NUMBER1, NUMBER3));
        GuardedString newPwd = new GuardedString("newPwd".toCharArray());
        facade.updateDelta(
                ObjectClass.ACCOUNT,
                bugs.getUid(),
                Set.of(delta, AttributeDeltaBuilder.buildPassword(newPwd)),
                null);

        bugs = facade.getObject(ObjectClass.ACCOUNT, bugs.getUid(), options);
        numberAttr = bugs.getAttributeByName("telephoneNumber").getValue();
        assertEquals(2, numberAttr.size());
        assertTrue(numberAttr.contains(NUMBER1));
        assertTrue(numberAttr.contains(NUMBER3));

        assertDoesNotThrow(() -> facade.authenticate(ObjectClass.ACCOUNT, USER01_CN, newPwd, null));
        facade.removeAttributeValues(ObjectClass.ACCOUNT, bugs.getUid(), Set.of(telephoneAttr), null);
    }

    @Test
    void simpleAddRemoveAttrs() {
        ConnectorFacade facade = newFacade();
        ConnectorObject bugs = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        Attribute number1 = AttributeBuilder.build("telephoneNumber", NUMBER1);

        Uid newUid = facade.addAttributeValues(ObjectClass.ACCOUNT, bugs.getUid(), Set.of(number1), null);

        OperationOptions options = new OperationOptionsBuilder().setAttributesToGet("telephoneNumber").build();

        bugs = facade.getObject(ObjectClass.ACCOUNT, newUid, options);
        List<Object> numberAttr = bugs.getAttributeByName("telephoneNumber").getValue();
        assertEquals(NUMBER1, numberAttr.get(0));
        assertEquals(1, numberAttr.size());

        Attribute number2 = AttributeBuilder.build("telephoneNumber", NUMBER2);
        newUid = facade.addAttributeValues(ObjectClass.ACCOUNT, bugs.getUid(), Set.of(number2), null);

        bugs = facade.getObject(ObjectClass.ACCOUNT, newUid, options);
        numberAttr = bugs.getAttributeByName("telephoneNumber").getValue();
        assertEquals(2, numberAttr.size());
        assertEquals(NUMBER1, numberAttr.get(0));
        assertEquals(NUMBER2, numberAttr.get(1));

        newUid = facade.removeAttributeValues(
                ObjectClass.ACCOUNT, bugs.getUid(), Set.of(number1, number2), null);

        bugs = facade.getObject(ObjectClass.ACCOUNT, newUid, options);
        assertTrue(bugs.getAttributeByName("telephoneNumber").getValue().isEmpty());
    }

    @Test
    void rename() {
        ConnectorFacade facade = newFacade();
        ConnectorObject bugs = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        Name name = new Name(DAFFY_DUCK_DN);
        Attribute number = AttributeBuilder.build("telephoneNumber", NUMBER1);
        Uid newUid = facade.update(ObjectClass.ACCOUNT, bugs.getUid(), Set.of(name, number), null);

        OperationOptionsBuilder builder = new OperationOptionsBuilder().setAttributesToGet("telephoneNumber");

        ConnectorObject daffy = facade.getObject(ObjectClass.ACCOUNT, newUid, builder.build());
        assertEquals(name, daffy.getName());
        assertEquals(NUMBER1, daffy.getAttributeByName("telephoneNumber").getValue().get(0));
        Attribute noNumber = AttributeBuilder.build("telephoneNumber");
        facade.update(ObjectClass.ACCOUNT, newUid, Set.of(new Name(USER01_DN), noNumber), null);
    }

    @Test
    void renameWhenUidNotDefault() {
        LdUpConfiguration config = newConfiguration();
        assertFalse(config.getUidAttribute().equalsIgnoreCase("entryDN"));
        config.setUidAttribute("entryDN");
        config.setGidAttribute("entryDN");
        config.setAidAttribute("entryDN");
        ConnectorFacade facade = newFacade(config);
        ConnectorObject bugs = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        Name name = new Name(DAFFY_DUCK_DN);
        Uid newUid = facade.update(ObjectClass.ACCOUNT, bugs.getUid(), Set.of(name), null);

        // Since they are both the entry DN.
        assertEquals(name.getNameValue(), newUid.getUidValue());
        ConnectorObject daffy = facade.getObject(ObjectClass.ACCOUNT, newUid, null);
        assertEquals(name, daffy.getName());
        facade.update(ObjectClass.ACCOUNT, newUid, Set.of(new Name(USER01_DN)), null);
    }

    @Test
    void emptyAttributeValueRemovesAttribute() {
        ConnectorFacade facade = newFacade();
        ConnectorObject bugs = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        facade.update(ObjectClass.ACCOUNT, bugs.getUid(),
                Set.of(AttributeBuilder.build("telephoneNumber", NUMBER1)), null);

        facade.update(ObjectClass.ACCOUNT, bugs.getUid(),
                Set.of(AttributeBuilder.build("telephoneNumber")), null);

        bugs = facade.getObject(ObjectClass.ACCOUNT, bugs.getUid(),
                new OperationOptionsBuilder().setAttributesToGet("telephoneNumber").build());
        assertTrue(bugs.getAttributeByName("telephoneNumber").getValue().isEmpty());
    }

    @Test
    void updateBinaryAttributes() throws IOException {
        ConnectorFacade facade = newFacade();
        ConnectorObject bugs = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        byte[] certificate;
        try (InputStream in = getClass().getResourceAsStream("/certificate.cert")) {
            certificate = IOUtil.inputStreamToBytes(in);
        }
        Attribute certAttr = AttributeBuilder.build("userCertificate;binary", List.of(certificate));
        Uid newUid = facade.update(ObjectClass.ACCOUNT, bugs.getUid(), Set.of(certAttr), null);

        byte[] photo;
        try (InputStream in = getClass().getResourceAsStream("/photo.jpg")) {
            photo = IOUtil.inputStreamToBytes(in);
        }
        Attribute photoAttr = AttributeBuilder.build("jpegPhoto", List.of(photo));
        newUid = facade.addAttributeValues(ObjectClass.ACCOUNT, newUid, Set.of(photoAttr), null);

        bugs = facade.getObject(ObjectClass.ACCOUNT, newUid, new OperationOptionsBuilder().
                setAttributesToGet("userCertificate;binary", "jpegPhoto").build());

        byte[] storedCertificate = (byte[]) bugs.getAttributeByName("userCertificate;binary").getValue().get(0);
        assertTrue(Arrays.equals(certificate, storedCertificate));

        byte[] storedPhoto = (byte[]) bugs.getAttributeByName("jpegPhoto").getValue().get(0);
        assertTrue(Arrays.equals(photo, storedPhoto));

        Attribute noCertificate = AttributeBuilder.build("userCertificate;binary");
        Attribute noPhoto = AttributeBuilder.build("jpegPhoto");
        facade.update(ObjectClass.ACCOUNT, newUid, Set.of(noCertificate, noPhoto), null);
    }

    @Test
    void renameDnAttribute() {
        ConnectorFacade facade = newFacade();

        Name renameOneName = new Name(RENAME_ONE_TEST_DN);
        Set<Attribute> attributes = Set.of(
                renameOneName,
                AttributeBuilder.build("uid", "rename.one"),
                AttributeBuilder.build("cn", "Rename"),
                AttributeBuilder.build("givenName", "Rename"),
                AttributeBuilder.build("sn", "Rename"));

        Uid uid = facade.create(ObjectClass.ACCOUNT, attributes, null);

        ConnectorObject renameOne = facade.getObject(ObjectClass.ACCOUNT, uid,
                new OperationOptionsBuilder().setAttributesToGet("uid").build());
        assertEquals(renameOneName, renameOne.getName());
        assertEquals("rename.one", renameOne.getAttributeByName("uid").getValue().get(0));

        Name name = new Name(RENAME_TWO_TEST_DN);
        Uid newUid = facade.update(ObjectClass.ACCOUNT, renameOne.getUid(),
                Set.of(name, AttributeBuilder.build("uid", "rename.one")), null);

        ConnectorObject renameTwo = facade.getObject(ObjectClass.ACCOUNT, newUid,
                new OperationOptionsBuilder().setAttributesToGet("uid").build());
        assertEquals(name, renameTwo.getName());
        assertEquals("rename.two", renameTwo.getAttributeByName("uid").getValue().get(0));

        facade.delete(ObjectClass.ACCOUNT, newUid, null);
    }
}
