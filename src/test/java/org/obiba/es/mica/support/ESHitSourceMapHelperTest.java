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

import org.assertj.core.util.Maps;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.json.JsonData;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
@Ignore
public class ESHitSourceMapHelperTest {

  ObjectMapper mapper = new ObjectMapper();

  @Test
  public void flattenMapSimple() {
    Map<String, JsonData> map = Maps.newHashMap("A", JsonData.of("bla"));
    Map<String, String> flattened = new HashMap<>();
    ESHitSourceMapHelper.flattenMap(mapper, mapper.valueToTree(map), flattened);
    assertTrue(map.size() == flattened.size());
    assertTrue(map.get("A").toString().equals(flattened.get("A")));
  }

  @Test
  public void flattenMapOneHierarchy() {
    Map<String, JsonData> mapB = Maps.newHashMap("B", JsonData.of("bla"));

    Map<String, JsonData> mapA = Maps.newHashMap("A", JsonData.of("blabla"));
    mapA.put("A1", JsonData.of(mapB));

    Map<String, String> flattened = new HashMap<>();
    ESHitSourceMapHelper.flattenMap(mapper, mapper.valueToTree(mapA), flattened);
    assertTrue(flattened.size() == 2);
    assertNotNull(flattened.get("A"));
    assertNotNull(flattened.get("A1.B"));
    assertTrue(flattened.get("A").equals("blabla"));
    assertTrue(flattened.get("A1.B").equals("bla"));
  }

  @Test
  public void flattenMapSeveralHierarchy() {
    Map<String, Object> mapD = Maps.newHashMap("D", "blabla");

    Map<String, Object> mapC = Maps.newHashMap("C", mapD);

    Map<String, Object> mapB = Maps.newHashMap("B", mapC);

    Map<String, JsonData> mapA = Maps.newHashMap("A", JsonData.of(mapB));
    mapA.put("A1", JsonData.of(mapC));
    mapA.put("A2", JsonData.of(mapD));

    Map<String, String> flattened = new HashMap<>();
    ESHitSourceMapHelper.flattenMap(mapper, mapper.valueToTree(mapA), flattened);
    assertTrue(flattened.size() == 3);
    assertTrue(flattened.get("A.B.C.D").equals("blabla"));
    assertTrue(flattened.get("A1.C.D").equals("blabla"));
    assertTrue(flattened.get("A2.D").equals("blabla"));
  }

}