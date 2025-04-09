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

import net.tirasa.connid.bundles.ldup.search.LdUpFilter;
import net.tirasa.connid.bundles.ldup.search.LdUpSearchOp;
import net.tirasa.connid.bundles.ldup.sync.LdUpLiveSyncOp;
import net.tirasa.connid.bundles.ldup.sync.LdUpSyncOp;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.LiveSyncResultsHandler;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.LiveSyncOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;

@ConnectorClass(configurationClass = LdUpConfiguration.class, displayNameKey = "ldup.connector.display")
public class LdUpConnector
        implements PoolableConnector, TestOp, SchemaOp, SearchOp<LdUpFilter>, SyncOp, LiveSyncOp {

    protected static final Log LOG = Log.getLog(LdUpConnector.class);

    protected LdUpUtils ldUpUtils;

    protected LdUpSchemaOp ldUpSchema;

    protected LdUpSearchOp ldUpSearchOp;

    protected LdUpSyncOp ldUpSync;

    protected LdUpLiveSyncOp ldUpLiveSync;

    @Override
    public LdUpConfiguration getConfiguration() {
        if (ldUpUtils == null) {
            throw new ConnectorException("No init performed yet");
        }
        return ldUpUtils.getConfiguration();
    }

    @Override
    public void init(final Configuration configuration) {
        ldUpUtils = new LdUpUtils((LdUpConfiguration) configuration);
        ldUpUtils.getConfiguration().validate();

        ldUpSchema = new LdUpSchemaOp(ldUpUtils);
        ldUpSearchOp = new LdUpSearchOp(ldUpUtils);
        ldUpSync = new LdUpSyncOp(ldUpUtils);
        ldUpLiveSync = new LdUpLiveSyncOp(ldUpUtils);
    }

    @Override
    public void test() {
        if (ldUpUtils == null) {
            throw new ConnectorException("No init performed yet");
        }

        try {
            ldUpUtils.getConnectionFactory().validate();
        } catch (Exception e) {
            throw new ConnectionFailedException(e);
        }
    }

    @Override
    public void checkAlive() {
        test();
    }

    @Override
    public void dispose() {
        if (ldUpUtils.getConnectionFactory().isInitialized()) {
            try {
                ldUpUtils.getConnectionFactory().close();
            } catch (Exception e) {
                LOG.error(e, "While closing the connection factory");
            }
        }
    }

    @Override
    public Schema schema() {
        return ldUpSchema.schema();
    }

    @Override
    public FilterTranslator<LdUpFilter> createFilterTranslator(
            final ObjectClass objectClass,
            final OperationOptions options) {

        return ldUpSearchOp.createFilterTranslator(objectClass, options);
    }

    @Override
    public void executeQuery(
            final ObjectClass objectClass,
            final LdUpFilter filter,
            final ResultsHandler handler,
            final OperationOptions options) {

        ldUpSearchOp.executeQuery(objectClass, filter, handler, options);
    }

    @Override
    public SyncToken getLatestSyncToken(final ObjectClass objectClass) {
        return ldUpSync.getLatestSyncToken(objectClass);
    }

    @Override
    public void sync(
            final ObjectClass objectClass,
            final SyncToken token,
            final SyncResultsHandler handler,
            final OperationOptions options) {

        ldUpSync.sync(objectClass, token, handler, options);
    }

    @Override
    public void livesync(
            final ObjectClass objectClass,
            final LiveSyncResultsHandler handler,
            final OperationOptions options) {

        ldUpLiveSync.livesync(objectClass, handler, options);
    }
}
