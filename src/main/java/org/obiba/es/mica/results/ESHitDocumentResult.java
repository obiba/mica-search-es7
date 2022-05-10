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
import org.obiba.mica.spi.search.Searcher.DocumentResult;

import co.elastic.clients.elasticsearch.core.search.Hit;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * {@link SearchHit} wrapper.
 */
public class ESHitDocumentResult implements Searcher.DocumentResult {
  private final Hit<DocumentResult> hit;

  public ESHitDocumentResult(Hit<DocumentResult> hit) {
    this.hit = hit;
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
    return hit.source().getSource();
  }

  @Override
  public InputStream getSourceInputStream() {
    return new ByteArrayInputStream(hit.source().toString().getBytes());
  }

  @Override
  public String getClassName() {
    if (!hasSource()) return null;
    Object className = hit.source().getSource().get("className");
    return className == null ? null : className.toString();
  }
}
