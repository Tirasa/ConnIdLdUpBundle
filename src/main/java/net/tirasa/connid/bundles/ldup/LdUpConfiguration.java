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

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

public class LdUpConfiguration extends AbstractConfiguration {

    private static final String DEFAULT_ID_ATTRIBUTE = "entryUUID";

    private String url;

    private boolean useStartTLS = false;

    private boolean autoReconnect = true;

    private int connectTimeoutSeconds = 30;

    private int responseTimeoutSeconds = 30;

    private String bindDn;

    private GuardedString bindPassword;

    private int poolMinSize = 1;

    private int poolMaxSize = 10;

    private String baseDn;

    private String accountObjectClass = "inetOrgPerson";

    private String groupObjectClass = "groupOfUniqueNames";

    private String uidAttribute = DEFAULT_ID_ATTRIBUTE;

    private String gidAttribute = DEFAULT_ID_ATTRIBUTE;

    private String aidAttribute = DEFAULT_ID_ATTRIBUTE;

    private String passwordAttribute = "userPassword";

    private String groupMemberAttribute = "uniqueMember";

    private boolean legacyCompatibilityMode = false;

    @ConfigurationProperty(displayMessageKey = "url.display",
            helpMessageKey = "url.help", required = true, order = 1)
    public String getUrl() {
        return url;
    }

    public void setUrl(final String url) {
        this.url = url;
    }

    @ConfigurationProperty(displayMessageKey = "useStartTLS.display",
            helpMessageKey = "urluseStartTLS.help", order = 2)
    public boolean isUseStartTLS() {
        return useStartTLS;
    }

    public void setUseStartTLS(final boolean useStartTLS) {
        this.useStartTLS = useStartTLS;
    }

    @ConfigurationProperty(displayMessageKey = "autoReconnect.display",
            helpMessageKey = "autoReconnect.help", order = 3)
    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public void setAutoReconnect(final boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    @ConfigurationProperty(displayMessageKey = "connectTimeoutSeconds.display",
            helpMessageKey = "connectTimeoutSeconds.help", required = true, order = 4)
    public int getConnectTimeoutSeconds() {
        return connectTimeoutSeconds;
    }

    public void setConnectTimeoutSeconds(final int connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
    }

    @ConfigurationProperty(displayMessageKey = "responseTimeoutSeconds.display",
            helpMessageKey = "responseTimeoutSeconds.help", required = true, order = 5)
    public int getResponseTimeoutSeconds() {
        return responseTimeoutSeconds;
    }

    public void setResponseTimeoutSeconds(final int responseTimeoutSeconds) {
        this.responseTimeoutSeconds = responseTimeoutSeconds;
    }

    @ConfigurationProperty(displayMessageKey = "bindDn.display",
            helpMessageKey = "bindDn.help", order = 6)
    public String getBindDn() {
        return bindDn;
    }

    public void setBindDn(final String bindDn) {
        this.bindDn = bindDn;
    }

    @ConfigurationProperty(displayMessageKey = "bindPassword.display",
            helpMessageKey = "bindPassword.help", confidential = true, order = 7)
    public GuardedString getBindPassword() {
        return bindPassword;
    }

    public void setBindPassword(final GuardedString bindPassword) {
        this.bindPassword = bindPassword;
    }

    @ConfigurationProperty(displayMessageKey = "poolMinSize.display",
            helpMessageKey = "poolMinSize.help", order = 8)
    public int getPoolMinSize() {
        return poolMinSize;
    }

    public void setPoolMinSize(final int poolMinSize) {
        this.poolMinSize = poolMinSize;
    }

    @ConfigurationProperty(displayMessageKey = "poolMaxSize.display",
            helpMessageKey = "poolMaxSize.help", order = 9)
    public int getPoolMaxSize() {
        return poolMaxSize;
    }

    public void setPoolMaxSize(final int poolMaxSize) {
        this.poolMaxSize = poolMaxSize;
    }

    @ConfigurationProperty(displayMessageKey = "baseDn.display",
            helpMessageKey = "baseDn.help", order = 10)
    public String getBaseDn() {
        return baseDn;
    }

    public void setBaseDn(final String baseDn) {
        this.baseDn = baseDn;
    }

    @ConfigurationProperty(displayMessageKey = "accountObjectClass.display",
            helpMessageKey = "accountObjectClass.help", required = true, order = 11)
    public String getAccountObjectClass() {
        return accountObjectClass;
    }

    public void setAccountObjectClass(final String accountObjectClass) {
        this.accountObjectClass = accountObjectClass;
    }

    @ConfigurationProperty(displayMessageKey = "groupObjectClass.display",
            helpMessageKey = "groupObjectClass.help", required = true, order = 12)
    public String getGroupObjectClass() {
        return groupObjectClass;
    }

    public void setGroupObjectClass(final String groupObjectClass) {
        this.groupObjectClass = groupObjectClass;
    }

    @ConfigurationProperty(displayMessageKey = "uidAttribute.display",
            helpMessageKey = "uidAttribute.help", required = true, order = 13)
    public String getUidAttribute() {
        return uidAttribute;
    }

    public void setUidAttribute(final String uidAttribute) {
        this.uidAttribute = uidAttribute;
    }

    @ConfigurationProperty(displayMessageKey = "gidAttribute.display",
            helpMessageKey = "gidAttribute.help", required = true, order = 14)
    public String getGidAttribute() {
        return gidAttribute;
    }

    public void setGidAttribute(final String gidAttribute) {
        this.gidAttribute = gidAttribute;
    }

    @ConfigurationProperty(displayMessageKey = "aidAttribute.display",
            helpMessageKey = "aidAttribute.help", required = true, order = 15)
    public String getAidAttribute() {
        return aidAttribute;
    }

    public void setAidAttribute(final String aidAttribute) {
        this.aidAttribute = aidAttribute;
    }

    @ConfigurationProperty(displayMessageKey = "passwordAttribute.display",
            helpMessageKey = "passwordAttribute.help", required = true, order = 16)
    public String getPasswordAttribute() {
        return passwordAttribute;
    }

    public void setPasswordAttribute(final String passwordAttribute) {
        this.passwordAttribute = passwordAttribute;
    }

    @ConfigurationProperty(displayMessageKey = "groupMemberAttribute.display",
            helpMessageKey = "groupMemberAttribute.help", required = true, order = 17)
    public String getGroupMemberAttribute() {
        return groupMemberAttribute;
    }

    public void setGroupMemberAttribute(final String groupMemberAttribute) {
        this.groupMemberAttribute = groupMemberAttribute;
    }

    @ConfigurationProperty(displayMessageKey = "legacyCompatibilityMode.display",
            helpMessageKey = "legacyCompatibilityMode.help", order = 18)
    public boolean isLegacyCompatibilityMode() {
        return legacyCompatibilityMode;
    }

    public void setLegacyCompatibilityMode(final boolean legacyCompatibilityMode) {
        this.legacyCompatibilityMode = legacyCompatibilityMode;
    }

    @Override
    public void validate() {
        if (StringUtil.isBlank(url)) {
            throw new ConfigurationException("Missing LDAP URL");
        }

        if (connectTimeoutSeconds < 0) {
            throw new ConfigurationException("Negative connectTimeoutSeconds");
        }
        if (responseTimeoutSeconds < 0) {
            throw new ConfigurationException("Negative responseTimeoutSeconds");
        }

        if ((StringUtil.isBlank(bindDn) && bindPassword != null)
                || (StringUtil.isNotBlank(bindDn) && bindPassword == null)) {

            throw new ConfigurationException("Bind DN and password must be both either null or non-null");
        }

        if (StringUtil.isBlank(baseDn)) {
            throw new ConfigurationException("Missing base DN");
        }
    }
}
