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

import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.ldaptive.LdapException;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchScope;
import org.ldaptive.SingleConnectionFactory;
import org.ldaptive.control.SyncDoneControl;
import org.ldaptive.control.util.DefaultCookieManager;
import org.ldaptive.control.util.SyncReplClient;

public class LdUpSyncOp extends AbstractLdUpSyncOp implements SyncOp {

    protected static final Log LOG = Log.getLog(LdUpSyncOp.class);

    public LdUpSyncOp(final LdUpUtils ldUpUtils) {
        super(ldUpUtils);
    }

    @Override
    public SyncToken getLatestSyncToken(final ObjectClass objectClass) {
        AtomicReference<String> latest = new AtomicReference<>();

        SingleConnectionFactory scf = new SingleConnectionFactory(ldUpUtils.getConnectionConfig());
        SyncReplClient client = new SyncReplClient(scf, false);
        try {
            scf.initialize();

            client.setOnResult(result -> {
                LOG.ok("SyncRepl result received: {0}", result);

                SyncDoneControl syncDoneControl = (SyncDoneControl) result.getControl(SyncDoneControl.OID);

                latest.set(Base64.getEncoder().encodeToString(syncDoneControl.getCookie()));
            });
            client.setOnException(e -> LOG.error(e, "SyncRepl exception thrown"));

            client.send(SearchRequest.builder().
                    dn(ldUpUtils.getConfiguration().getBaseDn()).
                    scope(SearchScope.SUBTREE).
                    filter("objectClass=" + ldUpUtils.ldapObjectClass(objectClass)).build(),
                    new DefaultCookieManager()).await();
        } catch (LdapException e) {
            throw new ConnectorException("While managing SyncRepl events for " + objectClass, e);
        } finally {
            client.close();
            scf.close();
        }

        return Optional.ofNullable(latest.get()).map(SyncToken::new).orElse(null);
    }

    @Override
    public void sync(
            final ObjectClass objectClass,
            final SyncToken token,
            final SyncResultsHandler handler,
            final OperationOptions options) {

        List<SyncDeltaBuilder> objects = dosync(
                objectClass,
                object -> new SyncDeltaBuilder().
                        setDeltaType(SyncDeltaType.CREATE_OR_UPDATE).
                        setObject(object.build()),
                object -> new SyncDeltaBuilder().
                        setDeltaType(SyncDeltaType.DELETE).
                        setObject(object.build()),
                (syncDelta, cookie) -> syncDelta.setToken(new SyncToken(cookie)),
                Optional.ofNullable(token).map(t -> Base64.getDecoder().decode(t.getValue().toString())).orElse(null),
                options);

        objects.forEach(object -> handler.handle(object.build()));
    }
}
