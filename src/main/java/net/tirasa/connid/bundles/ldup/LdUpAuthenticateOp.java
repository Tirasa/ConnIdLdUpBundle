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

import java.util.Optional;
import javax.security.auth.login.CredentialExpiredException;
import javax.security.auth.login.LoginException;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidCredentialException;
import org.identityconnectors.framework.common.exceptions.PasswordExpiredException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.operations.AuthenticateOp;
import org.identityconnectors.framework.spi.operations.ResolveUsernameOp;
import org.ldaptive.ConnectionFactory;
import org.ldaptive.Credential;
import org.ldaptive.LdapEntry;
import org.ldaptive.LdapException;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.SingleConnectionFactory;
import org.ldaptive.auth.AuthenticationRequest;
import org.ldaptive.auth.AuthenticationResponse;
import org.ldaptive.auth.Authenticator;
import org.ldaptive.auth.SearchDnResolver;
import org.ldaptive.auth.SimpleBindAuthenticationHandler;
import org.ldaptive.auth.User;
import org.ldaptive.control.PasswordExpiredControl;
import org.ldaptive.control.PasswordPolicyControl;
import org.ldaptive.control.ResponseControl;
import org.ldaptive.handler.ResultPredicate;

public class LdUpAuthenticateOp implements AuthenticateOp, ResolveUsernameOp {

    protected final LdUpUtils ldUpUtils;

    public LdUpAuthenticateOp(final LdUpUtils ldUpUtils) {
        this.ldUpUtils = ldUpUtils;
    }

    protected SearchDnResolver searchDnResolver(final ConnectionFactory factory) {
        return SearchDnResolver.builder().
                factory(factory).
                dn(ldUpUtils.getConfiguration().getBaseDn()).
                subtreeSearch(true).
                filter("(" + ldUpUtils.getConfiguration().getUidAttribute() + "={user})").
                build();
    }

    @Override
    public Uid authenticate(
            final ObjectClass objectClass,
            final String username,
            final GuardedString password,
            final OperationOptions options) {

        SingleConnectionFactory scf = new SingleConnectionFactory(ldUpUtils.getConnectionConfig());
        try {
            scf.initialize();

            Authenticator authenticator = new Authenticator(
                    searchDnResolver(scf),
                    new SimpleBindAuthenticationHandler(scf));
            AuthenticationResponse response = authenticator.authenticate(new AuthenticationRequest(
                    username,
                    new Credential(SecurityUtil.decrypt(password)),
                    new String[] { ldUpUtils.getConfiguration().getUidAttribute() }));
            if (response.isSuccess()) {
                LdapEntry entry = response.getLdapEntry();
                return new Uid(entry.getAttribute(ldUpUtils.getConfiguration().getUidAttribute()).getStringValue());
            } else {
                for (ResponseControl ctl : response.getControls()) {
                    if (ctl instanceof PasswordExpiredControl) {
                        throw new PasswordExpiredException();
                    }
                    if (ctl instanceof PasswordPolicyControl) {
                        ((PasswordPolicyControl) ctl).getError().throwSecurityException();
                    }
                }
                throw new InvalidCredentialException("Authentication failed: " + response.getDiagnosticMessage());
            }
        } catch (LoginException e) {
            if (e instanceof CredentialExpiredException) {
                throw new PasswordExpiredException(e);
            }
            throw new InvalidCredentialException(e);
        } catch (LdapException e) {
            throw new ConnectorException("While authenticating " + username, e);
        }
    }

    @Override
    public Uid resolveUsername(
            final ObjectClass objectClass,
            final String username,
            final OperationOptions options) {

        try {
            return Optional.ofNullable(searchDnResolver(ldUpUtils.getConnectionFactory()).resolve(new User(username))).
                    map(dn -> {
                        try {
                            SearchResponse response = SearchOperation.builder().
                                    factory(ldUpUtils.getConnectionFactory()).
                                    throwIf(ResultPredicate.NOT_SUCCESS).
                                    build().execute(SearchRequest.objectScopeSearchRequest(
                                            dn, new String[] { ldUpUtils.getConfiguration().getUidAttribute() }));
                            return new Uid(response.getEntry().
                                    getAttribute(ldUpUtils.getConfiguration().getUidAttribute()).getStringValue());
                        } catch (LdapException e) {
                            throw new ConnectorException("While reading "
                                    + ldUpUtils.getConfiguration().getUidAttribute() + " from " + dn, e);
                        }
                    }).
                    orElseThrow(() -> new InvalidCredentialException("Cannot resolve username " + username));
        } catch (LdapException e) {
            throw new ConnectorException("While resolving username " + username, e);
        }
    }
}
