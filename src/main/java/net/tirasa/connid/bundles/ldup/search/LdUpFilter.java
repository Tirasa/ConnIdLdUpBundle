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

import java.util.Objects;

/**
 * Encapsulates an LDAP filter.
 *
 * An instance of this class is consists of an optional entry DN and an optional native LDAP filter.
 * The semantics of such an instance is "an LDAP entry with this entry DN (if specified) and matching this
 * native filter (if specified)".
 */
public class LdUpFilter {

    public static LdUpFilter forEntryDN(final String entryDN) {
        return new LdUpFilter(null, entryDN);
    }

    public static LdUpFilter forNativeFilter(final String nativeFilter) {
        return new LdUpFilter(nativeFilter, null);
    }

    protected static String combine(final String left, final String right, final char op) {
        if (left != null) {
            if (right != null) {
                StringBuilder builder = new StringBuilder();
                builder.append('(');
                builder.append(op);
                builder.append(left);
                builder.append(right);
                builder.append(')');
                return builder.toString();
            }
            return left;
        }

        return right;
    }

    protected final String nativeFilter;

    protected final String entryDN;

    protected LdUpFilter(final String nativeFilter, final String entryDN) {
        this.nativeFilter = nativeFilter;
        this.entryDN = entryDN;
    }

    public LdUpFilter withNativeFilter(final String nativeFilter) {
        return new LdUpFilter(nativeFilter, this.entryDN);
    }

    /**
     * Logically "ANDs" together this filter with another filter.
     *
     * If at most one of the two filters has an entry DN, the result is a filter with that entry DN (if any) and a
     * native filter whose value is the native filters of the two filters "ANDed" together using the LDAP
     * {code}&{code} operator.
     *
     * Otherwise, the method returns null.
     *
     * @param other the other filter.
     *
     * @return the two filters "ANDed" together or null.
     */
    public LdUpFilter and(final LdUpFilter other) {
        if (entryDN == null || other.entryDN == null) {
            return new LdUpFilter(
                    combine(nativeFilter, other.nativeFilter, '&'),
                    entryDN == null ? other.entryDN : entryDN);
        }
        return null;
    }

    /**
     * Logically "ORs" together this filter with another filter.
     *
     * If none of the two filters has an entry DN, the result is a filter with no entry DN and a native filter whose
     * value is the native filters of the two filters "ORed" together using the LDAP {code}|{code} filter operator.
     *
     * Otherwise, the method returns null.
     *
     * @param other the other filter.
     *
     * @return the two filters "ORed" together or null.
     */
    public LdUpFilter or(final LdUpFilter other) {
        if (entryDN == null && other.entryDN == null) {
            return new LdUpFilter(
                    combine(nativeFilter, other.nativeFilter, '|'),
                    null);
        }
        return null;
    }

    public String getNativeFilter() {
        return nativeFilter;
    }

    public String getEntryDN() {
        return entryDN;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 83 * hash + Objects.hashCode(this.nativeFilter);
        hash = 83 * hash + Objects.hashCode(this.entryDN);
        return hash;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LdUpFilter other = (LdUpFilter) obj;
        if (!Objects.equals(this.nativeFilter, other.nativeFilter)) {
            return false;
        }
        return Objects.equals(this.entryDN, other.entryDN);
    }

    @Override
    public String toString() {
        return "LdUpFilter{"
                + "nativeFilter=" + nativeFilter
                + ", entryDN=" + entryDN
                + '}';
    }
}
