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

import static net.tirasa.connid.bundles.ldup.search.LdUpFilter.forEntryDN;
import static net.tirasa.connid.bundles.ldup.search.LdUpFilter.forNativeFilter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import net.tirasa.connid.bundles.ldup.AbstractLdUpConnectorTests;
import net.tirasa.connid.bundles.ldup.LdUpUtils;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.filter.ContainsAllValuesFilter;
import org.identityconnectors.framework.common.objects.filter.ContainsFilter;
import org.identityconnectors.framework.common.objects.filter.EndsWithFilter;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.EqualsIgnoreCaseFilter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.framework.common.objects.filter.GreaterThanFilter;
import org.identityconnectors.framework.common.objects.filter.GreaterThanOrEqualFilter;
import org.identityconnectors.framework.common.objects.filter.LessThanFilter;
import org.identityconnectors.framework.common.objects.filter.LessThanOrEqualFilter;
import org.identityconnectors.framework.common.objects.filter.StartsWithFilter;
import org.junit.jupiter.api.Test;

class LdUpFilterTranslatorTests extends AbstractLdUpConnectorTests {

    private static LdUpFilterTranslator newTranslator() {
        return new LdUpFilterTranslator(new LdUpUtils(newConfiguration()), ObjectClass.ACCOUNT);
    }

    @Test
    public void and() {
        assertEquals(forNativeFilter("(&(foo=1)(bar=2))"), newTranslator().createAndExpression(
                forNativeFilter("(foo=1)"),
                forNativeFilter("(bar=2)")));
        assertEquals(forEntryDN("o=isp").withNativeFilter("(foo=1)"), newTranslator().
                createAndExpression(
                        forEntryDN("o=isp"),
                        forNativeFilter("(foo=1)")));
        assertEquals(forEntryDN("o=isp").withNativeFilter("(&(foo=1)(bar=2))"), newTranslator().
                createAndExpression(
                        forEntryDN("o=isp").withNativeFilter("(foo=1)"),
                        forNativeFilter("(bar=2)")));
        assertNull(newTranslator().createAndExpression(
                forEntryDN("o=isp").withNativeFilter("(foo=1)"),
                forEntryDN("dc=example,dc=org").withNativeFilter("(bar=2)")));
    }

    @Test
    public void or() {
        assertEquals(forNativeFilter("(|(foo=1)(bar=2))"),
                newTranslator().createOrExpression(forNativeFilter("(foo=1)"), forNativeFilter(
                        "(bar=2)")));
        assertNull(newTranslator().createOrExpression(
                forEntryDN("o=isp"), forNativeFilter("(foo=1)")));
    }

    @Test
    public void contains() {
        ContainsFilter filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build("foo", ""));
        assertEquals(forNativeFilter("(foo=*)"), newTranslator().createContainsExpression(filter, false));

        filter = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build("foo", "bar"));
        assertEquals(forNativeFilter("(foo=*bar*)"), newTranslator().createContainsExpression(filter, false));
    }

    @Test
    public void startsWith() {
        StartsWithFilter filter = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build("foo", ""));
        assertEquals(forNativeFilter("(foo=*)"), newTranslator().createStartsWithExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo=*))"), newTranslator().createStartsWithExpression(filter, true));

        filter = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build("foo", "bar"));
        assertEquals(forNativeFilter("(foo=bar*)"), newTranslator().createStartsWithExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo=bar*))"), newTranslator().createStartsWithExpression(filter, true));
    }

    @Test
    public void endsWith() {
        EndsWithFilter filter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build("foo", ""));
        assertEquals(forNativeFilter("(foo=*)"), newTranslator().createEndsWithExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo=*))"), newTranslator().createEndsWithExpression(filter, true));

        filter = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build("foo", "bar"));
        assertEquals(forNativeFilter("(foo=*bar)"), newTranslator().createEndsWithExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo=*bar))"), newTranslator().createEndsWithExpression(filter, true));
    }

    @Test
    public void equals() {
        EqualsFilter filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build("foo"));
        assertNull(newTranslator().createEqualsExpression(filter, false));

        filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build("foo", ""));
        assertEquals(forNativeFilter("(foo=*)"), newTranslator().createEqualsExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo=*))"), newTranslator().createEqualsExpression(filter, true));

        filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build("foo", "bar"));
        assertEquals(forNativeFilter("(foo=bar)"), newTranslator().createEqualsExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo=bar))"), newTranslator().createEqualsExpression(filter, true));

        filter = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.build("foo", "bar", "baz"));
        assertEquals(forNativeFilter("(&(foo=bar)(foo=baz))"), newTranslator().createEqualsExpression(filter, false));
        assertEquals(forNativeFilter("(!(&(foo=bar)(foo=baz)))"), newTranslator().createEqualsExpression(filter, true));
    }

    @Test
    public void equalsIgnoreCase() {
        EqualsIgnoreCaseFilter filter = (EqualsIgnoreCaseFilter) FilterBuilder.equalsIgnoreCase(AttributeBuilder.build(
                "foo", ""));
        assertEquals(forNativeFilter("(foo=*)"), newTranslator().createEqualsIgnoreCaseExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo=*))"), newTranslator().createEqualsIgnoreCaseExpression(filter, true));

        filter = (EqualsIgnoreCaseFilter) FilterBuilder.equalsIgnoreCase(AttributeBuilder.build("foo", "bar"));
        assertEquals(forNativeFilter("(foo=bar)"), newTranslator().createEqualsIgnoreCaseExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo=bar))"), newTranslator().createEqualsIgnoreCaseExpression(filter, true));
    }

    @Test
    public void greaterThan() {
        GreaterThanFilter filter = (GreaterThanFilter) FilterBuilder.greaterThan(AttributeBuilder.build("foo", 42));
        assertEquals(forNativeFilter("(!(foo<=42))"), newTranslator().createGreaterThanExpression(filter, false));
        assertEquals(forNativeFilter("(foo<=42)"), newTranslator().createGreaterThanExpression(filter, true));
    }

    @Test
    public void greaterThanOrEqual() {
        GreaterThanOrEqualFilter filter = (GreaterThanOrEqualFilter) FilterBuilder.greaterThanOrEqualTo(
                AttributeBuilder.build("foo", 42));
        assertEquals(forNativeFilter("(foo>=42)"), newTranslator().createGreaterThanOrEqualExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo>=42))"), newTranslator().createGreaterThanOrEqualExpression(filter, true));
    }

    @Test
    public void lessThan() {
        LessThanFilter filter = (LessThanFilter) FilterBuilder.lessThan(AttributeBuilder.build("foo", 42));
        assertEquals(forNativeFilter("(!(foo>=42))"), newTranslator().createLessThanExpression(filter, false));
        assertEquals(forNativeFilter("(foo>=42)"), newTranslator().createLessThanExpression(filter, true));
    }

    @Test
    public void lessThanOrEqual() {
        LessThanOrEqualFilter filter = (LessThanOrEqualFilter) FilterBuilder.lessThanOrEqualTo(AttributeBuilder.build(
                "foo", 42));
        assertEquals(forNativeFilter("(foo<=42)"), newTranslator().createLessThanOrEqualExpression(filter, false));
        assertEquals(forNativeFilter("(!(foo<=42))"), newTranslator().createLessThanOrEqualExpression(filter, true));
    }

    @Test
    public void entryDN() {
        ContainsFilter contains = (ContainsFilter) FilterBuilder.contains(AttributeBuilder.build("entryDN",
                "o=isp"));
        assertEquals(forEntryDN("o=isp"), newTranslator().createContainsExpression(contains, false));

        StartsWithFilter startsWith = (StartsWithFilter) FilterBuilder.startsWith(AttributeBuilder.build("entryDN",
                "o=isp"));
        assertEquals(forEntryDN("o=isp"), newTranslator().createStartsWithExpression(startsWith, false));

        EndsWithFilter endsWith = (EndsWithFilter) FilterBuilder.endsWith(AttributeBuilder.build("entryDN",
                "o=isp"));
        assertEquals(forEntryDN("o=isp"), newTranslator().createEndsWithExpression(endsWith, false));

        EqualsFilter equals = (EqualsFilter) FilterBuilder.equalTo(AttributeBuilder.
                build("entryDN", "o=isp"));
        assertEquals(forEntryDN("o=isp"), newTranslator().createEqualsExpression(equals, false));

        ContainsAllValuesFilter containsAllValues = (ContainsAllValuesFilter) FilterBuilder.containsAllValues(
                AttributeBuilder.build("entryDN", "o=isp"));
        assertEquals(forEntryDN("o=isp"), newTranslator().createContainsAllValuesExpression(
                containsAllValues, false));

        containsAllValues = (ContainsAllValuesFilter) FilterBuilder.containsAllValues(AttributeBuilder.build("entryDN",
                "o=isp", "o=Acme,o=isp"));
        assertNull(newTranslator().createContainsAllValuesExpression(containsAllValues, false));
    }
}
