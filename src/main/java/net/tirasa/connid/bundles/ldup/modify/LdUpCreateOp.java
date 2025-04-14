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

import java.util.Optional;
import java.util.Set;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.ldaptive.AddOperation;
import org.ldaptive.AddRequest;
import org.ldaptive.AttributeModification;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapException;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.handler.ResultPredicate;

public class LdUpCreateOp extends AbstractLdUpModifyOp implements CreateOp {

    protected static final Log LOG = Log.getLog(LdUpCreateOp.class);

    public LdUpCreateOp(final LdUpUtils ldUpUtils) {
        super(ldUpUtils);
    }

    @Override
    public Uid create(
            final ObjectClass objectClass,
            final Set<Attribute> createAttributes,
            final OperationOptions options) {

        Name name = Optional.ofNullable(AttributeUtil.getNameFromAttributes(createAttributes)).
                orElseThrow(() -> new IllegalArgumentException("No Name attribute provided in the attributes"));

        ProcessResult result = process(objectClass, createAttributes);
        result.ldapAttrs.add(new LdapAttribute("objectClass", ldUpUtils.ldapObjectClass(objectClass)));

        // 1. create
        Uid uid;
        try {
            AddOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().
                    execute(AddRequest.builder().
                            dn(name.getNameValue()).
                            attributes(result.ldapAttrs).
                            build());

            SearchResponse response = SearchOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(SearchRequest.objectScopeSearchRequest(
                            name.getNameValue(), new String[] { ldUpUtils.getIdAttribute(objectClass) }));
            uid = new Uid(response.getEntry().
                    getAttribute(ldUpUtils.getIdAttribute(objectClass)).getStringValue());
        } catch (LdapException e) {
            throw new ConnectorException("Create error", e);
        }

        if (ldUpUtils.isAccount(objectClass)) {
            // 2. set password if provided
            setPassword(name.getNameValue(), result.passwordValue);

            // 3. set group memberships if provided
            groupMod(name.getNameValue(), result.groups, AttributeModification.Type.ADD);
        }

        return uid;
    }
}
