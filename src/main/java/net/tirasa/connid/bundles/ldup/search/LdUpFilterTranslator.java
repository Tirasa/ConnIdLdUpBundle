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

import java.util.List;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.AttributeFilter;
import org.identityconnectors.framework.common.objects.filter.ContainsAllValuesFilter;
import org.identityconnectors.framework.common.objects.filter.ContainsFilter;
import org.identityconnectors.framework.common.objects.filter.EndsWithFilter;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.EqualsIgnoreCaseFilter;
import org.identityconnectors.framework.common.objects.filter.GreaterThanFilter;
import org.identityconnectors.framework.common.objects.filter.GreaterThanOrEqualFilter;
import org.identityconnectors.framework.common.objects.filter.LessThanFilter;
import org.identityconnectors.framework.common.objects.filter.LessThanOrEqualFilter;
import org.identityconnectors.framework.common.objects.filter.SingleValueAttributeFilter;
import org.identityconnectors.framework.common.objects.filter.StartsWithFilter;

public class LdUpFilterTranslator extends AbstractFilterTranslator<LdUpFilter> {

    protected static StringBuilder createBuilder(final boolean not) {
        return new StringBuilder(not ? "(!(" : "(");
    }

    protected static LdUpFilter finishBuilder(final StringBuilder builder) {
        boolean not = builder.charAt(0) == '(' && builder.charAt(1) == '!';
        builder.append(not ? "))" : ")");
        return LdUpFilter.forNativeFilter(builder.toString());
    }

    private static boolean escapeByteArrayAttrValue(final byte[] value, final StringBuilder builder) {
        if (value.length == 0) {
            return false;
        }
        for (byte b : value) {
            builder.append('\\');
            String hex = Integer.toHexString(b & 0xff); // Make a negative byte positive.
            if (hex.length() < 2) {
                builder.append('0');
            }
            builder.append(hex);
        }
        return true;
    }

    private static boolean escapeStringAttrValue(final String value, final StringBuilder builder) {
        if (StringUtil.isEmpty(value)) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '*':
                    builder.append("\\2a");
                    break;
                case '(':
                    builder.append("\\28");
                    break;
                case ')':
                    builder.append("\\29");
                    break;
                case '\\':
                    builder.append("\\5c");
                    break;
                case '\0':
                    builder.append("\\00");
                    break;
                default:
                    builder.append(ch);
            }
        }
        return true;
    }

    /**
     * Escapes the given attribute value to the given {@code StringBuilder}.
     *
     * @param value value
     * @param toBuilder builder
     *
     * @return {@code true} if anything was written to the builder.
     */
    protected static boolean escapeAttrValue(final Object value, final StringBuilder toBuilder) {
        if (value == null) {
            return false;
        }
        if (value instanceof byte[]) {
            return escapeByteArrayAttrValue((byte[]) value, toBuilder);
        }
        return escapeStringAttrValue(value.toString(), toBuilder);
    }

    protected final LdUpUtils ldUpUtils;

    protected final ObjectClass objectClass;

    public LdUpFilterTranslator(final LdUpUtils ldUpUtils, final ObjectClass objectClass) {
        this.ldUpUtils = ldUpUtils;
        this.objectClass = objectClass;
    }

    @Override
    public LdUpFilter createAndExpression(final LdUpFilter leftExpression, final LdUpFilter rightExpression) {
        return leftExpression.and(rightExpression);
    }

    @Override
    public LdUpFilter createOrExpression(final LdUpFilter leftExpression, final LdUpFilter rightExpression) {
        return leftExpression.or(rightExpression);
    }

    @Override
    public LdUpFilter createContainsExpression(final ContainsFilter filter, final boolean not) {
        String attrName = ldUpUtils.getLdapAttribute(objectClass, filter.getAttribute().getName()).orElse(null);
        if (attrName == null) {
            return null;
        }

        if (LdUpUtils.isDNAttribute(attrName)) {
            return LdUpFilter.forEntryDN(filter.getValue());
        }

        StringBuilder builder = createBuilder(not).append(attrName).append('=').append('*');
        if (escapeAttrValue(filter.getValue(), builder)) {
            builder.append('*');
        }
        return finishBuilder(builder);
    }

    @Override
    public LdUpFilter createEndsWithExpression(final EndsWithFilter filter, final boolean not) {
        String attrName = ldUpUtils.getLdapAttribute(objectClass, filter.getAttribute().getName()).orElse(null);
        if (attrName == null) {
            return null;
        }

        if (LdUpUtils.isDNAttribute(attrName)) {
            return LdUpFilter.forEntryDN(filter.getValue());
        }

        StringBuilder builder = createBuilder(not).append(attrName).append('=').append('*');
        escapeAttrValue(filter.getValue(), builder);
        return finishBuilder(builder);
    }

    protected LdUpFilter createContainsAllValuesFilter(final AttributeFilter filter, final boolean not) {
        String attrName = ldUpUtils.getLdapAttribute(objectClass, filter.getAttribute().getName()).orElse(null);
        if (attrName == null) {
            return null;
        }
        List<Object> values = filter.getAttribute().getValue();
        if (values == null) {
            return null;
        }
        StringBuilder builder;
        switch (values.size()) {
            case 0:
                return null;

            case 1:
                Object single = values.get(0);
                if (single == null) {
                    return null;
                }
                if (LdUpUtils.isDNAttribute(attrName)) {
                    return LdUpFilter.forEntryDN(single.toString());
                }
                builder = createBuilder(not);
                addSimpleFilter(attrName, "=", values.get(0), builder);
                return finishBuilder(builder);

            default:
                if (LdUpUtils.isDNAttribute(attrName)) {
                    return null; // Because the DN is single-valued.
                }
                builder = createBuilder(not);
                boolean hasValue = false;
                builder.append('&');
                for (Object value : values) {
                    if (value != null) {
                        hasValue = true;
                        builder.append('(');
                        addSimpleFilter(attrName, "=", value, builder);
                        builder.append(')');
                    }
                }
                if (!hasValue) {
                    return null;
                }
                return finishBuilder(builder);
        }
    }

    @Override
    public LdUpFilter createEqualsExpression(final EqualsFilter filter, final boolean not) {
        return createContainsAllValuesFilter(filter, not);
    }

    @Override
    protected LdUpFilter createEqualsIgnoreCaseExpression(final EqualsIgnoreCaseFilter filter, final boolean not) {
        // LDAP is generally case-insensitive, reverting to EqualsFilter
        Attribute attr = filter.getValue() == null
                ? AttributeBuilder.build(filter.getName())
                : AttributeBuilder.build(filter.getName(), filter.getValue());
        return createEqualsExpression(new EqualsFilter(attr), not);
    }

    protected static void addSimpleFilter(
            final String ldapAttr,
            final String type,
            final Object value,
            final StringBuilder toBuilder) {

        toBuilder.append(ldapAttr).append(type);
        if (!escapeAttrValue(value, toBuilder)) {
            toBuilder.append('*');
        }
    }

    protected LdUpFilter createSingleValueFilter(
            final String type,
            final SingleValueAttributeFilter filter,
            final boolean not) {

        String attrName = ldUpUtils.getLdapAttribute(objectClass, filter.getAttribute().getName()).orElse(null);
        if (attrName == null) {
            return null;
        }

        if (LdUpUtils.isDNAttribute(attrName)) {
            return LdUpFilter.forEntryDN(filter.getValue().toString());
        }

        StringBuilder builder = createBuilder(not);
        addSimpleFilter(attrName, type, filter.getValue(), builder);
        return finishBuilder(builder);
    }

    @Override
    public LdUpFilter createGreaterThanExpression(final GreaterThanFilter filter, final boolean not) {
        return createSingleValueFilter("<=", filter, !not);
    }

    @Override
    public LdUpFilter createGreaterThanOrEqualExpression(final GreaterThanOrEqualFilter filter, final boolean not) {
        return createSingleValueFilter(">=", filter, not);
    }

    @Override
    public LdUpFilter createLessThanExpression(final LessThanFilter filter, final boolean not) {
        return createSingleValueFilter(">=", filter, !not);
    }

    @Override
    public LdUpFilter createLessThanOrEqualExpression(final LessThanOrEqualFilter filter, final boolean not) {
        return createSingleValueFilter("<=", filter, not);
    }

    @Override
    public LdUpFilter createStartsWithExpression(final StartsWithFilter filter, final boolean not) {
        String attrName = ldUpUtils.getLdapAttribute(objectClass, filter.getAttribute().getName()).orElse(null);
        if (attrName == null) {
            return null;
        }

        if (LdUpUtils.isDNAttribute(attrName)) {
            return LdUpFilter.forEntryDN(filter.getValue());
        }

        StringBuilder builder = createBuilder(not).append(attrName).append('=');
        escapeAttrValue(filter.getValue(), builder);
        builder.append('*');
        return finishBuilder(builder);
    }

    @Override
    public LdUpFilter createContainsAllValuesExpression(final ContainsAllValuesFilter filter, final boolean not) {
        return createContainsAllValuesFilter(filter, not);
    }
}
