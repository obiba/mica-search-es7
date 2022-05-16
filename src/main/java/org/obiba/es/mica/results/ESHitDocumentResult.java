/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.results;

import org.elasticsearch.search.SearchHit;
import org.obiba.mica.spi.search.Searcher;

import co.elastic.clients.elasticsearch.core.search.Hit;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * {@link SearchHit} wrapper.
 */
public class ESHitDocumentResult implements Searcher.DocumentResult {
  private final Hit<ObjectNode> hit;
  private final ObjectMapper objectMapper;

  public ESHitDocumentResult(Hit<ObjectNode> hit, ObjectMapper objectMapper) {
    this.hit = hit;
    this.objectMapper = objectMapper;
  }

  @Override
  public String getId() {
    return hit.id();
  }

  @Override
  public boolean hasSource() {
    return hit.source() != null;
  }

  @Override
  public Map<String, Object> getSource() {
    ObjectNode source = hit.source();
    if (source.isObject()) {
      return objectMapper.convertValue(source.objectNode(), new TypeReference<Map<String, Object>>() {});
    } else {
      return new HashMap<>();
    }
  }

  @Override
  public InputStream getSourceInputStream() {
    return new ByteArrayInputStream(hit.source().toString().getBytes());
  }

  @Override
  public String getClassName() {
    if (!hasSource()) return null;
    Object className = getSource().get("className");
    return className == null ? null : className.toString();
  }
}
