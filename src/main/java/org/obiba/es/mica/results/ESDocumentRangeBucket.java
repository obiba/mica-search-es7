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

import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.obiba.mica.spi.search.Searcher;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link Range.Bucket} aggregation wrapper.
 */
public class ESDocumentRangeBucket implements Searcher.DocumentRangeBucket {
  private final Range.Bucket bucket;

  public ESDocumentRangeBucket(Range.Bucket bucket) {
    this.bucket = bucket;
  }

  @Override
  public String getKeyAsString() {
    return bucket.getKeyAsString();
  }

  @Override
  public long getDocCount() {
    return bucket.getDocCount();
  }

  @Override
  public Double getFrom() {
    return (Double) bucket.getFrom();
  }

  @Override
  public Double getTo() {
    return (Double) bucket.getTo();
  }

  @Override
  public List<Searcher.DocumentAggregation> getAggregations() {
    return bucket.getAggregations().asList().stream().map(ESDocumentAggregation::new).collect(Collectors.toList());
  }
}
