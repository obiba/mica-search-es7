/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.support;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.obiba.mica.spi.search.support.AggregationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class AggregationParser {

  private static final Logger log = LoggerFactory.getLogger(AggregationParser.class);

  private List<String> locales;

  private long minDocCount = 0;

  public AggregationParser() {
  }

  public void setLocales(List<String> locales) {
    this.locales = locales;
  }

  public Iterable<AbstractAggregationBuilder> getAggregations(@Nullable Properties properties) {
    return getAggregations(properties, null);
  }

  public Iterable<AbstractAggregationBuilder> getAggregations(@Nullable Properties properties,
                                                              @Nullable Map<String, Properties> subProperties) {
    if (properties == null) return Collections.emptyList();

    Map<String, Iterable<AbstractAggregationBuilder>> subAggregations = Maps.newHashMap();
    if (subProperties != null) {
      subProperties.forEach((key, subs) -> subAggregations.put(key, parseAggregations(subs, null)));
    }

    return parseAggregations(properties, subAggregations);
  }

  private Iterable<AbstractAggregationBuilder> parseAggregations(@Nullable Properties properties,
                                                                 Map<String, Iterable<AbstractAggregationBuilder>> subAggregations) {
    Collection<AbstractAggregationBuilder> termsBuilders = new ArrayList<>();
    if (properties == null) return termsBuilders;

    SortedMap<String, ?> sortedSystemProperties = new TreeMap(properties);
    String prevKey = null;
    for (Map.Entry<String, ?> entry : sortedSystemProperties.entrySet()) {
      String key = entry.getKey().replaceAll("\\" + AggregationHelper.PROPERTIES + ".*$", "");
      if (!key.equals(prevKey)) {
        parseAggregation(termsBuilders, properties, key, subAggregations);
        prevKey = key;
      }
    }

    return termsBuilders;
  }

  private void parseAggregation(Collection<AbstractAggregationBuilder> termsBuilders, Properties properties,
                                String key, Map<String, Iterable<AbstractAggregationBuilder>> subAggregations) {
    Boolean localized = Boolean.valueOf(properties.getProperty(key + AggregationHelper.LOCALIZED));
    String aliasProperty = properties.getProperty(key + AggregationHelper.ALIAS);
    String typeProperty = properties.getProperty(key + AggregationHelper.TYPE);
    List<String> types = null == typeProperty ? Arrays.asList(AggregationHelper.AGG_STERMS) : Arrays.asList(typeProperty.split(","));
    List<String> aliases = null == aliasProperty ? Arrays.asList("") : Arrays.asList(aliasProperty.split(","));

    IntStream.range(0, types.size()).forEach(i -> {
      String aggType = getAggregationType(types.get(i), localized);
      getFields(key, aliases.get(i), localized).entrySet().forEach(entry -> {
        log.trace("Building aggregation '{}' of type '{}'", entry.getKey(), aggType);

        switch (aggType) {
          case AggregationHelper.AGG_STERMS:
            TermsAggregationBuilder termBuilder = AggregationBuilders.terms(entry.getKey()).field(entry.getValue());
            if (minDocCount > -1) termBuilder.minDocCount(minDocCount);
            if (subAggregations != null && subAggregations.containsKey(entry.getValue())) {
              subAggregations.get(entry.getValue()).forEach(termBuilder::subAggregation);
            }
            termsBuilders.add(termBuilder.order(BucketOrder.key(true)).size(Short.MAX_VALUE));
            break;
          case AggregationHelper.AGG_STATS:
            termsBuilders.add(AggregationBuilders.stats(entry.getKey()).field(entry.getValue()));
            break;
          case AggregationHelper.AGG_RANGE:
            RangeAggregationBuilder builder = AggregationBuilders.range(entry.getKey()).field(entry.getValue());
            Stream.of(properties.getProperty(key + AggregationHelper.RANGES).split(",")).forEach(range -> {
              String[] values = range.split(":");
              if (values.length != 2) throw new IllegalArgumentException("Range From and To are not defined");

              if (!"*".equals(values[0]) || !"*".equals(values[1])) {

                if ("*".equals(values[0])) {
                  builder.addUnboundedTo(range, Double.valueOf(values[1]));
                } else if ("*".equals(values[1])) {
                  builder.addUnboundedFrom(range, Double.valueOf(values[0]));
                } else {
                  builder.addRange(range, Double.valueOf(values[0]), Double.valueOf(values[1]));
                }
              }
            });
            if (subAggregations != null && subAggregations.containsKey(entry.getValue())) {
              subAggregations.get(entry.getValue()).forEach(agg -> builder.subAggregation(agg));
            }
            termsBuilders.add(builder);
            break;
          default:
            throw new IllegalArgumentException("Invalid aggregation type detected: " + aggType);
        }
      });
    });
  }

  private Map<String, String> getFields(String field, String alias, Boolean localized) {
    String name = AggregationHelper.formatName(Strings.isNullOrEmpty(alias) ? field : alias);
    final Map<String, String> fields = new HashMap<>();
    if (localized) {
      fields.put(name + AggregationHelper.UND_LOCALE_NAME, field + AggregationHelper.UND_LOCALE_FIELD);

      if (locales != null) {
        locales
            .forEach(locale -> fields.put(name + AggregationHelper.NAME_SEPARATOR + locale,
                field + AggregationHelper.FIELD_SEPARATOR + locale));
      } else {
        fields.put(name + AggregationHelper.DEFAULT_LOCALE_NAME, field + AggregationHelper.DEFAULT_LOCALE_FIELD);
      }
    } else {
      fields.put(name, field);
    }

    return fields;
  }

  /**
   * Default the type to 'terms' if localized is true, otherwise use valid input type
   *
   * @param type
   * @param localized
   * @return
   */
  private String getAggregationType(String type, Boolean localized) {
    return !localized && !Strings.isNullOrEmpty(type) && type.matches(String.format("^(%s|%s|%s)$", AggregationHelper.AGG_STATS, AggregationHelper.AGG_TERMS, AggregationHelper.AGG_RANGE))
        ? type
        : AggregationHelper.AGG_STERMS;
  }
}
