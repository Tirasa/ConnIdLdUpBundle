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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.common.security.SecurityUtil;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeInfo.Flags;
import org.identityconnectors.framework.common.objects.AttributeInfoBuilder;
import org.identityconnectors.framework.common.objects.AttributeUtil;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.LiveSyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.LiveSyncResultsHandler;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ObjectClassInfoBuilder;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.LiveSyncOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.ldaptive.BindConnectionInitializer;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.LdapEntry;
import org.ldaptive.LdapException;
import org.ldaptive.PooledConnectionFactory;
import org.ldaptive.ReturnAttributes;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResponse;
import org.ldaptive.SearchScope;
import org.ldaptive.SimpleBindRequest;
import org.ldaptive.SingleConnectionFactory;
import org.ldaptive.control.SyncDoneControl;
import org.ldaptive.control.SyncStateControl;
import org.ldaptive.control.util.DefaultCookieManager;
import org.ldaptive.control.util.SyncReplClient;
import org.ldaptive.extended.SyncInfoMessage;
import org.ldaptive.pool.BindConnectionPassivator;
import org.ldaptive.schema.AttributeUsage;
import org.ldaptive.schema.ObjectClassType;
import org.ldaptive.schema.SchemaFactory;

@ConnectorClass(configurationClass = LdUpConfiguration.class, displayNameKey = "ldup.connector.display")
public class LdUpConnector
        implements PoolableConnector, SchemaOp, SyncOp, LiveSyncOp, TestOp {

    protected static final Log LOG = Log.getLog(LdUpConnector.class);

    protected static final String SYNCREPL_COOKIE_NAME = AttributeUtil.createSpecialName("SYNCREPL_COOKIE");

    protected static final Set<String> NON_RETURN_ATTRS = Set.of(Uid.NAME, Name.NAME, SYNCREPL_COOKIE_NAME);

    protected LdUpConfiguration configuration;

    protected PooledConnectionFactory connectionFactory;

    protected Schema schema;

    @Override
    public LdUpConfiguration getConfiguration() {
        return configuration;
    }

    protected ConnectionConfig connectionConfig() {
        ConnectionConfig.Builder builder = ConnectionConfig.builder().
                url(configuration.getUrl()).
                useStartTLS(configuration.isUseStartTLS()).
                autoReconnect(configuration.isAutoReconnect()).
                connectTimeout(Duration.ofSeconds(configuration.getConnectTimeoutSeconds())).
                responseTimeout(Duration.ofSeconds(configuration.getResponseTimeoutSeconds()));

        if (StringUtil.isNotBlank(configuration.getBindDn())) {
            BindConnectionInitializer.Builder bcib = BindConnectionInitializer.builder().
                    dn(configuration.getBindDn());
            Optional.ofNullable(configuration.getBindPassword()).map(SecurityUtil::decrypt).
                    ifPresent(bcib::credential);
            builder.connectionInitializers(bcib.build());
        }

        return builder.build();
    }

    @Override
    public void init(final Configuration configuration) {
        this.configuration = (LdUpConfiguration) configuration;
        this.configuration.validate();

        connectionFactory = PooledConnectionFactory.builder().
                config(connectionConfig()).
                min(this.configuration.getPoolMinSize()).
                max(this.configuration.getPoolMaxSize()).
                validateOnCheckOut(true).
                validatePeriodically(true).
                build();

        if (StringUtil.isNotBlank(this.configuration.getBindDn())) {
            SimpleBindRequest.Builder builder = SimpleBindRequest.builder().
                    dn(this.configuration.getBindDn());
            Optional.ofNullable(this.configuration.getBindPassword()).map(SecurityUtil::decrypt).
                    ifPresent(builder::password);
            connectionFactory.setPassivator(new BindConnectionPassivator(builder.build()));
        }

        connectionFactory.initialize();
    }

    @Override
    public void test() {
        if (connectionFactory == null) {
            throw new ConnectorException("No init performed yet");
        }

        try {
            connectionFactory.validate();
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
        if (connectionFactory.isInitialized()) {
            try {
                connectionFactory.close();
            } catch (Exception e) {
                LOG.error(e, "While closing the connection factory");
            }
        }
    }

    protected static Optional<AttributeInfo> toAttributeInfo(
            final org.ldaptive.schema.Schema ldapSchema,
            final String attr,
            final Optional<Flags> add) {

        return Optional.ofNullable(ldapSchema.getAttributeType(attr)).map(attrType -> {
            Set<Flags> flags = EnumSet.noneOf(Flags.class);

            if (!attrType.isSingleValued()) {
                flags.add(Flags.MULTIVALUED);
            }
            if (attrType.isNoUserModification() || "objectClass".equalsIgnoreCase(attr)) {
                flags.add(Flags.NOT_CREATABLE);
                flags.add(Flags.NOT_UPDATEABLE);
            }
            if (attrType.getUsage() != AttributeUsage.USER_APPLICATIONS) {
                flags.add(Flags.NOT_RETURNED_BY_DEFAULT);
            }

            add.ifPresent(flags::add);

            return AttributeInfoBuilder.build(
                    attr,
                    Arrays.binarySearch(ldapSchema.getBinaryAttributeNames(), attr) < 0 ? String.class : byte[].class,
                    flags);
        });
    }

    @Override
    public Schema schema() {
        if (schema == null) {
            SchemaBuilder schemaBld = new SchemaBuilder(getClass());

            try {
                org.ldaptive.schema.Schema ldapSchema = SchemaFactory.createSchema(connectionFactory);

                ldapSchema.getObjectClasses().forEach(ldapClass -> {
                    ObjectClassInfoBuilder objClassBld = new ObjectClassInfoBuilder();
                    objClassBld.setType(ldapClass.getName());
                    objClassBld.setAuxiliary(ldapClass.getObjectClassType() == ObjectClassType.AUXILIARY);
                    // Any LDAP object class can contain sub-entries
                    objClassBld.setContainer(ldapClass.getObjectClassType() == ObjectClassType.STRUCTURAL);

                    Set<String> required = Optional.ofNullable(ldapClass.getRequiredAttributes()).
                            map(c -> Stream.of(c).collect(Collectors.toSet())).
                            orElseGet(() -> Set.of());
                    Set<String> optional = Optional.ofNullable(ldapClass.getOptionalAttributes()).
                            map(c -> Stream.of(c).collect(Collectors.toSet())).
                            orElseGet(() -> new HashSet<>());
                    // OpenLDAP's ipProtocol has MUST ( ... $ description ) MAY ( description )
                    optional.removeAll(required);

                    Set<AttributeInfo> attrInfos = new HashSet<>();
                    required.forEach(attr -> toAttributeInfo(ldapSchema, attr, Optional.of(Flags.REQUIRED)).
                            ifPresentOrElse(
                                    attrInfos::add,
                                    () -> LOG.warn("Could not find attribute {0} in object classes {1}",
                                            attr, ldapClass.getName())));
                    optional.forEach(attr -> toAttributeInfo(ldapSchema, attr, Optional.empty()).
                            ifPresentOrElse(
                                    attrInfos::add,
                                    () -> LOG.warn("Could not find attribute {0} in object classes {1}",
                                            attr, ldapClass.getName())));
                    objClassBld.addAllAttributeInfo(attrInfos);

                    schemaBld.defineObjectClass(objClassBld.build());
                });
            } catch (Exception e) {
                LOG.error(e, "While building schema");
            }

            schema = schemaBld.build();
        }

        return schema;
    }

    protected Optional<Set<String>> returnAttributes(final OperationOptions options) {
        return Optional.ofNullable(options.getAttributesToGet()).
                map(attrs -> Stream.of(attrs).
                filter(attr -> !NON_RETURN_ATTRS.contains(attr)).
                map(attr -> OperationalAttributes.PASSWORD_NAME.equals(attr)
                ? configuration.getPasswordAttribute() : attr).
                collect(Collectors.toSet()));
    }

    protected void copyAttributes(final LdapEntry entry, final ConnectorObjectBuilder object) {
        entry.getAttributes().forEach(attr -> {
            if (configuration.getPasswordAttribute().equals(attr.getName())) {
                object.addAttribute(AttributeBuilder.buildPassword(
                        new GuardedString(attr.getStringValue().toCharArray())));
            } else {
                object.addAttribute(AttributeBuilder.build(
                        attr.getName(),
                        attr.isBinary() ? attr.getBinaryValues() : attr.getStringValues()));
            }
        });
    }

    @Override
    public SyncToken getLatestSyncToken(final ObjectClass objectClass) {
        AtomicReference<String> latest = new AtomicReference<>();

        SingleConnectionFactory scf = new SingleConnectionFactory(connectionConfig());
        SyncReplClient client = new SyncReplClient(scf, false);
        try {
            scf.initialize();

            client.setOnResult(result -> {
                LOG.ok("SyncRepl result received: {0}", result);

                SyncDoneControl syncDoneControl = (SyncDoneControl) result.getControl(SyncDoneControl.OID);

                latest.set(new String(syncDoneControl.getCookie()));
            });
            client.setOnException(e -> LOG.error(e, "SyncRepl exception thrown"));

            client.send(SearchRequest.builder().
                    dn(configuration.getBaseDn()).
                    scope(SearchScope.SUBTREE).
                    filter("objectClass=" + objectClass.getObjectClassValue()).build(),
                    new DefaultCookieManager()).await();
        } catch (LdapException e) {
            throw new ConnectorException("While managing SyncRepl events for " + objectClass, e);
        } finally {
            client.close();
            scf.close();
        }

        return Optional.ofNullable(latest.get()).map(SyncToken::new).orElse(null);
    }

    protected <T> List<T> dosync(
            final ObjectClass objectClass,
            final Function<ConnectorObjectBuilder, T> createOrUpdate,
            final Function<ConnectorObjectBuilder, T> delete,
            final BiConsumer<T, String> outCookieReporter,
            final byte[] cookie,
            final OperationOptions options) {

        List<T> objects = new ArrayList<>();

        SingleConnectionFactory scf = new SingleConnectionFactory(connectionConfig());
        SyncReplClient client = new SyncReplClient(scf, false);
        try {
            scf.initialize();

            client.setOnEntry(entry -> {
                LOG.ok("SyncRepl entry received: {0}", entry);

                SyncStateControl ssc = (SyncStateControl) entry.getControl(SyncStateControl.OID);

                ConnectorObjectBuilder object = new ConnectorObjectBuilder().
                        setObjectClass(objectClass).
                        setUid(new Uid(ssc.getEntryUuid().toString())).
                        setName(entry.getDn());

                switch (ssc.getSyncState()) {
                    case ADD:
                    case MODIFY:
                        copyAttributes(entry, object);
                        objects.add(createOrUpdate.apply(object));
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
                            SearchResponse response = new SearchOperation(connectionFactory).execute(
                                    SearchRequest.builder().
                                            dn(configuration.getBaseDn()).
                                            scope(SearchScope.SUBTREE).
                                            filter("entryUUID=" + entryUUID.toString()).
                                            returnAttributes(ReturnAttributes.NONE.value()).
                                            build());
                            if (response.isSuccess()) {
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
                            } else {
                                LOG.warn("Unsuccessful response while searching for entryUUID={0}: {1}",
                                        entryUUID, response);
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

                objects.forEach(object -> outCookieReporter.accept(object, new String(syncDoneControl.getCookie())));
            });
            client.setOnException(e -> LOG.error(e, "SyncRepl exception thrown"));

            SearchRequest.Builder searchRequestBuilder = SearchRequest.builder().
                    dn(configuration.getBaseDn()).
                    scope(SearchScope.SUBTREE).
                    filter("objectClass=" + objectClass.getObjectClassValue());
            returnAttributes(options).ifPresent(searchRequestBuilder::returnAttributes);

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
                Optional.ofNullable(token).map(t -> t.getValue().toString().getBytes()).orElse(null),
                options);

        objects.forEach(object -> handler.handle(object.build()));
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
                (object, cookie) -> object.addAttribute(AttributeBuilder.build(SYNCREPL_COOKIE_NAME, cookie)),
                Optional.ofNullable(options.getPagedResultsCookie()).map(String::getBytes).orElse(null),
                options);

        objects.forEach(object -> handler.handle(new LiveSyncDeltaBuilder().setObject(object.build()).build()));
    }
}
