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
package net.tirasa.connid.bundles.ldup.sync;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;
import org.ldaptive.LdapException;
import org.ldaptive.ReturnAttributes;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.SearchScope;
import org.ldaptive.SingleConnectionFactory;
import org.ldaptive.control.SyncDoneControl;
import org.ldaptive.control.SyncStateControl;
import org.ldaptive.control.util.DefaultCookieManager;
import org.ldaptive.control.util.SyncReplClient;
import org.ldaptive.extended.SyncInfoMessage;
import org.ldaptive.handler.ResultPredicate;

abstract class LdUpAbstractSyncOp {

    protected static final Log LOG = Log.getLog(LdUpAbstractSyncOp.class);

    protected final LdUpUtils ldUpUtils;

    protected LdUpAbstractSyncOp(final LdUpUtils ldUpUtils) {
        this.ldUpUtils = ldUpUtils;
    }

    protected <T> List<T> dosync(
            final ObjectClass objectClass,
            final Function<ConnectorObjectBuilder, T> createOrUpdate,
            final Function<ConnectorObjectBuilder, T> delete,
            final BiConsumer<T, String> outCookieReporter,
            final byte[] cookie,
            final OperationOptions options) {

        List<T> objects = new ArrayList<>();

        SingleConnectionFactory scf = new SingleConnectionFactory(ldUpUtils.getConnectionConfig());
        SyncReplClient client = new SyncReplClient(scf, false);
        try {
            scf.initialize();

            client.setOnEntry(entry -> {
                LOG.ok("SyncRepl entry received: {0}", entry);

                SyncStateControl ssc = (SyncStateControl) entry.getControl(SyncStateControl.OID);

                switch (ssc.getSyncState()) {
                    case ADD:
                    case MODIFY:
                        objects.add(createOrUpdate.apply(
                                ldUpUtils.connectorObjectBuilder(
                                        objectClass,
                                        new Uid(ssc.getEntryUuid().toString()),
                                        entry,
                                        options)));
                        break;

                    // this is never reported with persist == false 
                    case DELETE:
                    default:
                        LOG.warn("Unsupported condition: SyncStateControl {0}", ssc);
                }
            });
            client.setOnMessage(message -> {
                LOG.ok("SyncRepl message received: {0}", message);

                if (message.getMessageType() == SyncInfoMessage.Type.SYNC_ID_SET) {
                    message.getEntryUuids().forEach(entryUUID -> {
                        try {
                            SearchResponse response = SearchOperation.builder().
                                    factory(ldUpUtils.getConnectionFactory()).
                                    throwIf(ResultPredicate.NOT_SUCCESS).
                                    build().execute(
                                            SearchRequest.builder().
                                                    dn(ldUpUtils.getConfiguration().getBaseDn()).
                                                    scope(SearchScope.SUBTREE).
                                                    filter("entryUUID=" + entryUUID.toString()).
                                                    returnAttributes(ReturnAttributes.NONE.value()).
                                                    build());
                            if (response.getEntries().isEmpty()) {
                                LOG.ok("No match while searching for entryUUID={0}: it was a DELETE", entryUUID);

                                ConnectorObjectBuilder object = new ConnectorObjectBuilder().
                                        setObjectClass(objectClass).
                                        setUid(new Uid(entryUUID.toString())).
                                        setName(entryUUID.toString());
                                objects.add(delete.apply(object));
                            } else {
                                LOG.ok("Match found while searching for entryUUID={0}: discard", entryUUID);
                            }
                        } catch (LdapException e) {
                            LOG.warn(e, "Error while searching for entryUUID={0}", entryUUID);
                        }
                    });
                }
            });
            client.setOnResult(result -> {
                LOG.ok("SyncRepl result received: {0}", result);

                SyncDoneControl syncDoneControl = (SyncDoneControl) result.getControl(SyncDoneControl.OID);

                objects.forEach(object -> outCookieReporter.accept(
                        object, Base64.getEncoder().encodeToString(syncDoneControl.getCookie())));
            });
            client.setOnException(e -> LOG.error(e, "SyncRepl exception thrown"));

            SearchRequest.Builder searchRequestBuilder = SearchRequest.builder().
                    dn(ldUpUtils.getConfiguration().getBaseDn()).
                    scope(SearchScope.SUBTREE).
                    filter("objectClass=" + ldUpUtils.ldapObjectClass(objectClass));
            ldUpUtils.returnAttributes(options).ifPresent(searchRequestBuilder::returnAttributes);

            DefaultCookieManager cookieManager = new DefaultCookieManager();
            Optional.ofNullable(cookie).ifPresent(cookieManager::writeCookie);

            client.send(searchRequestBuilder.build(), cookieManager).await();
        } catch (LdapException e) {
            throw new ConnectorException("While managing SyncRepl events for " + objectClass, e);
        } finally {
            client.close();
            scf.close();
        }

        return objects;
    }
}
