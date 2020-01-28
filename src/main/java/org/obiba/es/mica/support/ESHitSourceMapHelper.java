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
import org.elasticsearch.search.SearchHit;

import java.util.Map;

final public class ESHitSourceMapHelper {

  public static Map<String, String> flattenMap(SearchHit hit) {
    Map<String, Object> source = hit.getSourceAsMap();
    Map<String, String> flattenedMap = Maps.newHashMap();
    flattenMap(source, flattenedMap, "");
    return flattenedMap;
  }

  public static void flattenMap(Map<String, Object> source, Map<String, String> flattened) {
    flattenMap(source, flattened, "");
  }

  /**
   * ES source filtering returns a hierarchy of HashMaps(attributes => label => en => "bla"). This helper flattens the
   * map to "attributes.label.en" => "bla".
   *
   * @param source
   * @param flattened
   * @param key
   */
  private static void flattenMap(Map<String, Object> source, Map<String, String> flattened, String key) {
    source.entrySet().stream().forEach(entry -> {
      Object value = entry.getValue();
      if (value instanceof Map) {
        flattenMap((Map)value, flattened, addPrefix(key, entry.getKey()));
      } else {
        flattened.put(addPrefix(key, entry.getKey()), (String)value);
      }
    });
  }

  private static String addPrefix(String key, String value) {
    return Strings.isNullOrEmpty(key) ? value :  key + "." + value;
  }
}
