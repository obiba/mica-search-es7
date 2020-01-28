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

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
@Ignore
public class ESHitSourceMapHelperTest {

  @Test
  public void flattenMapSimple() {
    Map<String, Object> map = Maps.newHashMap();
    map.put(("A"), "bla");
    Map<String, String> flattened = Maps.newHashMap();
    ESHitSourceMapHelper.flattenMap(map, flattened);
    assertTrue(map.size() == flattened.size());
    assertTrue(map.get("A").equals(flattened.get("A")));
  }

  @Test
  public void flattenMapOneHierarchy() {
    Map<String, Object> mapB = Maps.newHashMap();
    mapB.put("B", "bla");

    Map<String, Object> mapA = Maps.newHashMap();
    mapA.put("A", "blabla");
    mapA.put("A1", mapB);

    Map<String, String> flattened = Maps.newHashMap();
    ESHitSourceMapHelper.flattenMap(mapA, flattened);
    assertTrue(flattened.size() == 2);
    assertNotNull(flattened.get("A"));
    assertNotNull(flattened.get("A1.B"));
    assertTrue(flattened.get("A").equals("blabla"));
    assertTrue(flattened.get("A1.B").equals("bla"));
  }

  @Test
  public void flattenMapSeveralHierarchy() {
    Map<String, Object> mapD = Maps.newHashMap();
    mapD.put("D", "blabla");

    Map<String, Object> mapC = Maps.newHashMap();
    mapC.put("C", mapD);

    Map<String, Object> mapB = Maps.newHashMap();
    mapB.put("B", mapC);

    Map<String, Object> mapA = Maps.newHashMap();
    mapA.put("A", mapB);
    mapA.put("A1", mapC);
    mapA.put("A2", mapD);

    Map<String, String> flattened = Maps.newHashMap();
    ESHitSourceMapHelper.flattenMap(mapA, flattened);
    assertTrue(flattened.size() == 3);
    assertTrue(flattened.get("A.B.C.D").equals("blabla"));
    assertTrue(flattened.get("A1.C.D").equals("blabla"));
    assertTrue(flattened.get("A2.D").equals("blabla"));
  }

}