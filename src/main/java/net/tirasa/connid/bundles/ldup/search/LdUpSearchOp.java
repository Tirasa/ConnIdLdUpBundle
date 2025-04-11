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
package net.tirasa.connid.bundles.ldup.search;

import java.util.Base64;
import java.util.Optional;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.SearchResult;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.SearchResultsHandler;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.ldaptive.LdapException;
import org.ldaptive.ReturnAttributes;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.SearchScope;
import org.ldaptive.control.PagedResultsControl;
import org.ldaptive.handler.ResultPredicate;

public class LdUpSearchOp implements SearchOp<LdUpFilter> {

    protected static final Log LOG = Log.getLog(LdUpSearchOp.class);

    protected final LdUpUtils ldUpUtils;

    public LdUpSearchOp(final LdUpUtils ldUpUtils) {
        this.ldUpUtils = ldUpUtils;
    }

    @Override
    public FilterTranslator<LdUpFilter> createFilterTranslator(
            final ObjectClass objectClass,
            final OperationOptions options) {

        return new LdUpFilterTranslator(ldUpUtils, objectClass);
    }

    @Override
    public void executeQuery(
            final ObjectClass objectClass,
            final LdUpFilter filter,
            final ResultsHandler handler,
            final OperationOptions options) {

        LdUpFilter actualFilter = Optional.ofNullable(filter).
                orElseGet(() -> LdUpFilter.forNativeFilter("objectClass=" + ldUpUtils.ldapObjectClass(objectClass)));
        if (actualFilter.getEntryDN() == null && actualFilter.getNativeFilter() == null) {
            throw new ConnectorException("Invalid search filter");
        }

        SearchRequest request;
        if (actualFilter.getEntryDN() == null) {
            String nativeFilter = actualFilter.getNativeFilter();
            while (nativeFilter.startsWith("(") && nativeFilter.endsWith(")")) {
                nativeFilter = nativeFilter.substring(1, nativeFilter.length() - 1);
            }

            request = SearchRequest.builder().
                    dn(ldUpUtils.getConfiguration().getBaseDn()).
                    scope(Optional.ofNullable(options.getScope()).map(scope -> {
                        switch (scope) {
                            case OperationOptions.SCOPE_OBJECT:
                                return SearchScope.OBJECT;

                            case OperationOptions.SCOPE_ONE_LEVEL:
                                return SearchScope.ONELEVEL;

                            case OperationOptions.SCOPE_SUBTREE:
                            default:
                                return SearchScope.SUBTREE;
                        }
                    }).orElse(SearchScope.SUBTREE)).
                    filter("(&(objectClass=" + ldUpUtils.ldapObjectClass(objectClass) + ")(" + nativeFilter + "))").
                    build();
        } else {
            request = SearchRequest.builder().
                    dn(actualFilter.getEntryDN()).
                    scope(SearchScope.OBJECT).
                    filter("objectClass=" + ldUpUtils.ldapObjectClass(objectClass)).
                    build();
        }

        if (options.getPageSize() != null) {
            PagedResultsControl prc = new PagedResultsControl(options.getPageSize());
            Optional.ofNullable(options.getPagedResultsCookie()).
                    map(cookie -> Base64.getDecoder().decode(cookie)).ifPresent(prc::setCookie);
            request.setControls(prc);
        }

        String idAttr = ldUpUtils.getIdAttribute(objectClass);
        request.setReturnAttributes(ldUpUtils.returnAttributes(options).
                map(attrs -> {
                    attrs.add(idAttr);
                    return attrs.toArray(String[]::new);
                }).
                orElse(ReturnAttributes.ALL.value()));

        LOG.ok("Search request is {0}", request);

        try {
            SearchResponse response = SearchOperation.builder().
                    factory(ldUpUtils.getConnectionFactory()).
                    throwIf(ResultPredicate.NOT_SUCCESS).
                    build().execute(request);

            response.getEntries().forEach(entry -> {
                Uid uid = Optional.ofNullable(entry.getAttribute(idAttr)).
                        map(attr -> new Uid(attr.getStringValue())).
                        orElseThrow(() -> new IllegalArgumentException("Could not fetch " + idAttr + " value"));

                handler.handle(ldUpUtils.connectorObjectBuilder(objectClass, uid, entry, options).build());
            });

            if (handler instanceof SearchResultsHandler) {
                Optional.ofNullable(response.getControl(PagedResultsControl.OID)).
                        map(PagedResultsControl.class::cast).
                        filter(control -> control.getCookie() != null).
                        ifPresent(control -> ((SearchResultsHandler) handler).
                        handleResult(new SearchResult(
                                Base64.getEncoder().encodeToString(control.getCookie()), control.getSize())));
            }
        } catch (LdapException e) {
            LOG.warn(e, "Error while executing search request {0}", request);
        }
    }
}
