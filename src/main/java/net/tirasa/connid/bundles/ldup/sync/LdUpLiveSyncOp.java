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
import java.util.function.Function;
import net.tirasa.connid.bundles.ldup.LdUpConstants;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.LiveSyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.LiveSyncResultsHandler;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.spi.operations.LiveSyncOp;

public class LdUpLiveSyncOp extends AbstractLdUpSyncOp implements LiveSyncOp {

    public LdUpLiveSyncOp(final LdUpUtils ldUpUtils) {
        super(ldUpUtils);
    }

    @Override
    public void livesync(
            final ObjectClass objectClass,
            final LiveSyncResultsHandler handler,
            final OperationOptions options) {

        List<ConnectorObjectBuilder> objects = dosync(
                objectClass,
                Function.identity(),
                Function.identity(),
                (object, cookie) -> object.addAttribute(AttributeBuilder.build(
                        LdUpConstants.SYNCREPL_COOKIE_NAME, cookie)),
                Optional.ofNullable(options.getPagedResultsCookie()).
                        map(cookie -> Base64.getDecoder().decode(cookie)).orElse(null),
                options);

        objects.forEach(object -> handler.handle(new LiveSyncDeltaBuilder().setObject(object.build()).build()));
    }
}
