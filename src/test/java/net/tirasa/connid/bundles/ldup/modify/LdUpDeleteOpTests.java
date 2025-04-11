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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import net.tirasa.connid.bundles.ldup.AbstractLdUpConnectorTests;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.junit.jupiter.api.Test;

class LdUpDeleteOpTests extends AbstractLdUpConnectorTests {

    @Test
    void cannotDeleteExistingUidButWrongObjectClass() {
        ConnectorFacade facade = newFacade();
        ConnectorObject account = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        // Should fail because the object class passed to delete() is not ORGANIZATION.
        assertThrows(ConnectorException.class, () -> facade.delete(new ObjectClass("device"), account.getUid(), null));
    }

    @Test
    void delete() {
        ConnectorFacade facade = newFacade();
        ConnectorObject account = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).
                orElseThrow();

        facade.delete(ObjectClass.ACCOUNT, account.getUid(), null);

        assertTrue(searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).isEmpty());

        Set<Attribute> attributes = Set.of(
                new Name(USER01_DN),
                AttributeBuilder.build("uid", USER01_CN),
                AttributeBuilder.build("cn", USER01_CN),
                AttributeBuilder.build("sn", "Bar01"),
                AttributeBuilder.buildPassword(new GuardedString("carrot".toCharArray())));

        facade.create(ObjectClass.ACCOUNT, attributes, null);
    }
}
