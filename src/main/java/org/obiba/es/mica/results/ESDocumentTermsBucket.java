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

import org.obiba.mica.spi.search.Searcher;

import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link StringTermsBucket} wrapper.
 */
public class ESDocumentTermsBucket implements Searcher.DocumentTermsBucket {
  private final StringTermsBucket bucket;

  public ESDocumentTermsBucket(StringTermsBucket bucket) {
    this.bucket = bucket;
  }

  @Override
  public long getDocCount() {
    return bucket.docCount();
  }

  @Override
  public String getKeyAsString() {
    return bucket.key();
  }

  @Override
  public List<Searcher.DocumentAggregation> getAggregations() {
    return bucket.aggregations().values().stream().map(ESDocumentAggregation::new).collect(Collectors.toList());
  }
}
