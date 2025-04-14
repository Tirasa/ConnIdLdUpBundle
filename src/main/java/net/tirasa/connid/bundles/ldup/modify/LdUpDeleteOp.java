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

import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.ldaptive.AttributeModification;
import org.ldaptive.DeleteOperation;
import org.ldaptive.DeleteRequest;
import org.ldaptive.LdapException;
import org.ldaptive.handler.ResultPredicate;

public class LdUpDeleteOp extends AbstractLdUpModifyOp implements DeleteOp {

    public LdUpDeleteOp(final LdUpUtils ldUpUtils) {
        super(ldUpUtils);
    }

    @Override
    public void delete(
            final ObjectClass objectClass,
            final Uid uid,
            final OperationOptions options) {

        String dn = findDn(objectClass, uid);

        if (ldUpUtils.isAccount(objectClass)) {
            groupMod(dn, findGroups(dn), AttributeModification.Type.DELETE);
        }

        try {
            DeleteOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(DeleteRequest.builder().
                            dn(dn).
                            build());
        } catch (LdapException e) {
            throw new ConnectorException("While deleting " + dn, e);
        }
    }
}
