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

import java.util.HashSet;
import java.util.Set;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.ldaptive.AttributeModification;
import org.ldaptive.DeleteOperation;
import org.ldaptive.DeleteRequest;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapException;
import org.ldaptive.ModifyOperation;
import org.ldaptive.ModifyRequest;
import org.ldaptive.ReturnAttributes;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.SearchScope;
import org.ldaptive.auth.SearchDnResolver;
import org.ldaptive.auth.User;
import org.ldaptive.handler.ResultPredicate;

public class LdUpDeleteOp implements DeleteOp {

    protected static final Log LOG = Log.getLog(LdUpDeleteOp.class);

    protected final LdUpUtils ldUpUtils;

    public LdUpDeleteOp(final LdUpUtils ldUpUtils) {
        this.ldUpUtils = ldUpUtils;
    }

    @Override
    public void delete(
            final ObjectClass objectClass,
            final Uid uid,
            final OperationOptions options) {

        String idAttr = ldUpUtils.getIdAttribute(objectClass);
        String dn = null;
        if (LdUpUtils.isDNAttribute(idAttr)) {
            dn = uid.getUidValue();
        } else {
            try {
                SearchDnResolver dnResolver = SearchDnResolver.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        dn(ldUpUtils.getConfiguration().getBaseDn()).
                        subtreeSearch(true).
                        filter("(" + idAttr + "={user})").
                        build();
                dn = dnResolver.resolve(new User(uid.getUidValue()));
            } catch (LdapException e) {
                throw new ConnectorException("While resolving dn for " + uid.getUidValue(), e);
            }
        }

        if (dn == null) {
            throw new UnknownUidException(uid, objectClass);
        }

        try {
            SearchResponse response = SearchOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(
                            SearchRequest.builder().
                                    dn(dn).
                                    scope(SearchScope.OBJECT).
                                    filter("objectClass=" + ldUpUtils.ldapObjectClass(objectClass)).
                                    returnAttributes(ReturnAttributes.NONE.value()).
                                    build());
            if (response.getEntries().isEmpty()) {
                throw new ConnectorException("No entry found for " + dn
                        + " and objectClass" + ldUpUtils.ldapObjectClass(objectClass));
            }
        } catch (LdapException e) {
            throw new ConnectorException("While reading " + dn, e);
        }

        if (ldUpUtils.isAccount(objectClass)) {
            Set<String> groups = new HashSet<>();

            try {
                SearchResponse response = SearchOperation.builder().
                        factory(ldUpUtils.getConnectionFactory()).
                        throwIf(ResultPredicate.NOT_SUCCESS).
                        build().execute(
                                SearchRequest.builder().
                                        dn(ldUpUtils.getConfiguration().getBaseDn()).
                                        scope(SearchScope.SUBTREE).
                                        filter("(&(objectClass="
                                                + ldUpUtils.getConfiguration().getGroupObjectClass() + ")"
                                                + "(" + ldUpUtils.getConfiguration().getGroupMemberAttribute()
                                                + "=" + dn + "))").
                                        returnAttributes(ReturnAttributes.NONE.value()).
                                        build());
                groups.addAll(response.getEntryDns());
            } catch (LdapException e) {
                throw new ConnectorException("While searching for " + dn + " group memberships", e);
            }

            for (String group : groups) {
                try {
                    ModifyOperation.builder().
                            factory(ldUpUtils.getConnectionFactory()).
                            throwIf(ResultPredicate.NOT_SUCCESS).
                            build().execute(ModifyRequest.builder().
                                    dn(group).
                                    modifications(new AttributeModification(
                                            AttributeModification.Type.DELETE,
                                            new LdapAttribute(
                                                    ldUpUtils.getConfiguration().getGroupMemberAttribute(),
                                                    dn))).
                                    build());
                } catch (LdapException e) {
                    throw new ConnectorException("While removing " + dn + " from " + group, e);
                }
            }
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
