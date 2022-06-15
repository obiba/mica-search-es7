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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import co.elastic.clients.elasticsearch.core.search.Hit;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

final public class ESHitSourceMapHelper {

  private static Entry<String, JsonNode> next;

  public static Map<String, String> flattenMap(ObjectMapper mapper, Hit<ObjectNode> hit) {
    ObjectNode source = hit.source();
    Map<String, String> flattenedMap = Maps.newHashMap();
    flattenMap(mapper, source, flattenedMap, "");
    return flattenedMap;
  }

  public static void flattenMap(ObjectMapper mapper, ObjectNode source, Map<String, String> flattened) {
    flattenMap(mapper, source, flattened, "");
  }

  /**
   * ES source filtering returns a hierarchy of HashMaps(attributes => label => en => "bla"). This helper flattens the
   * map to "attributes.label.en" => "bla".
   *
   * @param source
   * @param flattened
   * @param key
   */
  private static void flattenMap(ObjectMapper mapper, ObjectNode source, Map<String, String> flattened, String key) {
    Iterator<Entry<String, JsonNode>> fields = source.fields();

    while (fields.hasNext()) {
      next = fields.next();
      JsonNode value = next.getValue();

      if (value.isObject()) {        
        flattenMap(mapper, mapper.valueToTree(value), flattened, addPrefix(key, next.getKey()));
      } else {
        flattened.put(addPrefix(key, next.getKey()), value.textValue());
      }
    }
  }

  private static String addPrefix(String key, String value) {
    return Strings.isNullOrEmpty(key) ? value :  key + "." + value;
  }
}
