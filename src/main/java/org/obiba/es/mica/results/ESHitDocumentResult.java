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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * {@link SearchHit} wrapper.
 */
public class ESHitDocumentResult implements Searcher.DocumentResult {
  private final SearchHit hit;

  public ESHitDocumentResult(SearchHit hit) {
    this.hit = hit;
  }

  @Override
  public String getId() {
    return hit.getId();
  }

  @Override
  public boolean hasSource() {
    return hit.getSourceRef() != null;
  }

  @Override
  public Map<String, Object> getSource() {
    return hit.getSourceAsMap();
  }

  @Override
  public InputStream getSourceInputStream() {
    return new ByteArrayInputStream(hit.getSourceAsString().getBytes());
  }

  @Override
  public String getClassName() {
    if (!hasSource()) return null;
    Object className = hit.getSourceAsMap().get("className");
    return className == null ? null : className.toString();
  }
}
