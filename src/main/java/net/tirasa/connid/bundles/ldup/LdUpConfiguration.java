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
