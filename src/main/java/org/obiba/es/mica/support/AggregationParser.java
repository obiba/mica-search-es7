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
import org.obiba.mica.spi.search.support.AggregationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationRange;
import co.elastic.clients.elasticsearch._types.aggregations.RangeAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.StatsAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;

import javax.annotation.Nullable;
import java.util.Arrays;
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

  public Map<String, Aggregation> getAggregations(@Nullable Properties properties, @Nullable Map<String, Properties> subProperties) {
    Map<String, Aggregation> aggregations = new HashMap<>();
    if (properties == null) return aggregations;

    SortedMap<String, ?> sortedSystemProperties = new TreeMap(properties);
    String prevKey = null;
    for (Map.Entry<String, ?> entry : sortedSystemProperties.entrySet()) {
      String key = entry.getKey().replaceAll("\\" + AggregationHelper.PROPERTIES + ".*$", "");
      if (!key.equals(prevKey)) {
        Map<String, Aggregation> parsedAggregationMap = parseAggregation(key, properties, subProperties);
        aggregations.putAll(parsedAggregationMap);
        prevKey = key;
      }
    }

    return aggregations;
  }

  private Map<String, Aggregation> parseAggregation(String key, Properties properties, @Nullable Map<String, Properties> subProperties) {
    Boolean localized = Boolean.valueOf(properties.getProperty(key + AggregationHelper.LOCALIZED));
    String aliasProperty = properties.getProperty(key + AggregationHelper.ALIAS);
    String typeProperty = properties.getProperty(key + AggregationHelper.TYPE);
    List<String> types = null == typeProperty ? Arrays.asList(AggregationHelper.AGG_STERMS) : Arrays.asList(typeProperty.split(","));
    List<String> aliases = null == aliasProperty ? Arrays.asList("") : Arrays.asList(aliasProperty.split(","));

    Map<String, Aggregation> parsed = new HashMap<>();

    IntStream.range(0, types.size()).forEach(i -> {
      String aggType = getAggregationType(types.get(i), localized);
      getFields(key, aliases.get(i), localized).entrySet().forEach(entry -> {
        log.trace("Building aggregation '{}' of type '{}'", entry.getKey(), aggType);

        switch (aggType) {
          case AggregationHelper.AGG_STERMS:
            String termsEntryValue = entry.getValue();
            String termsEntryKey = entry.getKey();
            int minDocCountAsInt = Long.valueOf(minDocCount).intValue();

            TermsAggregation termsAggregation = TermsAggregation.of(a -> a.field(termsEntryValue).size(Short.toUnsignedInt(Short.MAX_VALUE)).minDocCount(minDocCountAsInt > -1 ? minDocCountAsInt : 0));

            if (subProperties != null && subProperties.containsKey(termsEntryValue)) {
              Map<String, Aggregation> parsedSubAggregations = getAggregations(subProperties.get(termsEntryValue), null);
              parsedSubAggregations.remove(termsEntryValue);

              parsed.put(termsEntryKey, Aggregation.of(a -> a.terms(termsAggregation).aggregations(parsedSubAggregations)));
            } else {
              parsed.put(termsEntryKey, termsAggregation._toAggregation());
            }

            break;
          case AggregationHelper.AGG_STATS:
            parsed.put(entry.getKey(), StatsAggregation.of(a -> a.field(entry.getValue()))._toAggregation());
            break;
          case AggregationHelper.AGG_RANGE:
            String rangeEntryValue = entry.getValue();
            String rangeEntryKey = entry.getKey();

            RangeAggregation.Builder rangeAggregationBuilder = new RangeAggregation.Builder().field(rangeEntryValue);

            Stream.of(properties.getProperty(key + AggregationHelper.RANGES).split(",")).forEach(range -> {
              String[] values = range.split(":");
              if (values.length != 2) throw new IllegalArgumentException("Range From and To are not defined");

              if (!"*".equals(values[0]) || !"*".equals(values[1])) {
                if ("*".equals(values[0])) {
                  rangeAggregationBuilder.ranges(AggregationRange.of(a -> a.to(values[1])));
                } else if ("*".equals(values[1])) {
                  rangeAggregationBuilder.ranges(AggregationRange.of(a -> a.from(values[0])));
                } else {
                  rangeAggregationBuilder.ranges(AggregationRange.of(a -> a.from(values[0]).to(values[1])));
                }
              }

            });

            if (subProperties != null && subProperties.containsKey(rangeEntryValue)) {
              Map<String, Aggregation> parsedSubAggregations = getAggregations(subProperties.get(rangeEntryValue), null);
              parsed.put(rangeEntryKey, Aggregation.of(a -> a.range(rangeAggregationBuilder.build()).aggregations(parsedSubAggregations)));
            } else {
              parsed.put(rangeEntryKey, rangeAggregationBuilder.build()._toAggregation());
            }

            break;

        }
      });

    });

    return parsed;
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
