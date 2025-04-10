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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorSecurityException;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.ldaptive.LdapException;
import org.ldaptive.extended.ExtendedOperation;
import org.ldaptive.extended.PasswordModifyRequest;
import org.ldaptive.handler.ResultPredicate;

class LdUpAuthenticateOpTests extends AbstractLdUpConnectorTests {

    @BeforeAll
    public static void setPassword() throws LdapException {
        ExtendedOperation.builder().
                factory(singleConnectionFactory()).
                throwIf(ResultPredicate.NOT_SUCCESS).
                build().
                execute(new PasswordModifyRequest(USER01_DN, null, "carrot"));
    }

    @Test
    void withCnOrUid() {
        LdUpConfiguration config = newConfiguration();
        config.setUidAttribute("cn");
        ConnectorFacade facade = newFacade(config);
        ConnectorObject bugs = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        Uid uid = facade.resolveUsername(ObjectClass.ACCOUNT, USER01_CN, null);
        assertEquals(bugs.getUid(), uid);

        uid = facade.authenticate(ObjectClass.ACCOUNT, USER01_CN, new GuardedString("carrot".toCharArray()), null);
        assertEquals(bugs.getUid(), uid);

        config.setUidAttribute("uid");
        facade = newFacade(config);
        bugs = searchByAttribute(facade, ObjectClass.ACCOUNT, new Name(USER01_DN)).orElseThrow();

        uid = facade.resolveUsername(ObjectClass.ACCOUNT, USER01_CN, null);
        assertEquals(bugs.getUid(), uid);

        uid = facade.authenticate(ObjectClass.ACCOUNT, USER01_CN, new GuardedString("carrot".toCharArray()), null);
        assertEquals(bugs.getUid(), uid);
    }

    @Test
    void authenticateInvalidPassword() {
        ConnectorFacade facade = newFacade();
        assertThrows(
                ConnectorSecurityException.class,
                () -> facade.authenticate(
                        ObjectClass.ACCOUNT, USER01_CN, new GuardedString("rabbithole".toCharArray()), null));
    }

    @Test
    void unknownAccount() {
        ConnectorFacade facade = newFacade();
        try {
            facade.authenticate(
                    ObjectClass.ACCOUNT,
                    "hopefully.inexisting.user",
                    new GuardedString("none".toCharArray()),
                    null);
            fail();
        } catch (ConnectorSecurityException e) {
        }
        try {
            facade.resolveUsername(
                    ObjectClass.ACCOUNT,
                    "hopefully.inexisting.user",
                    null);
            fail();
        } catch (ConnectorSecurityException e) {
        }
    }
}
