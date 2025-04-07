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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.AssertionsKt.assertNotNull;

import java.util.EnumSet;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.junit.jupiter.api.Test;

class LdUpBasicTests extends AbstractLdUpConnectorTests {

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
        assertEquals(EnumSet.of(AttributeInfo.Flags.MULTIVALUED), uid.getFlags());

        assertTrue(inetOrgPerson.getAttributeInfo().stream().
                filter(ai -> PredefinedAttributes.GROUPS_NAME.equalsIgnoreCase(ai.getName())).findFirst().isPresent());

        ObjectClassInfo account = schema.getObjectClassInfo().stream().
                filter(oci -> ObjectClass.ACCOUNT_NAME.equals(oci.getType())).findFirst().orElseThrow();
        assertEquals(inetOrgPerson.getAttributeInfo(), account.getAttributeInfo());

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
