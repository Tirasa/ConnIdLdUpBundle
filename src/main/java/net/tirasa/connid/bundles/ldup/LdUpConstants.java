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

import java.util.Set;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.PredefinedAttributes;
import org.identityconnectors.framework.common.objects.Uid;

public final class LdUpConstants {

    public static final String DEFAULT_ID_ATTRIBUTE = "entryUUID";

    public static final String MEMBERS_ATTR_NAME = AttributeUtil.createSpecialName("MEMBERS");

    public static final String LEGACY_GROUPS_ATTR_NAME = "ldapGroups";

    public static final String SYNCREPL_COOKIE_NAME = AttributeUtil.createSpecialName("SYNCREPL_COOKIE");

    public static final Set<String> NON_RETURN_ATTRS = Set.of(
            Uid.NAME, Name.NAME, PredefinedAttributes.GROUPS_NAME, LEGACY_GROUPS_ATTR_NAME, SYNCREPL_COOKIE_NAME);

    private LdUpConstants() {
        // private constructor for static utility class
    }
}
