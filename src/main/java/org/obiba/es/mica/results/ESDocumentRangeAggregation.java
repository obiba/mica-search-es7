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

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.obiba.mica.spi.search.Searcher;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link Range} aggregation wrapper.
 */
public class ESDocumentRangeAggregation implements Searcher.DocumentRangeAggregation {
  private final Range range;

  public ESDocumentRangeAggregation(Aggregation range) {
    this.range = (Range) range;
  }

  @Override
  public List<Searcher.DocumentRangeBucket> getBuckets() {
    return range.getBuckets().stream().map(ESDocumentRangeBucket::new).collect(Collectors.toList());
  }
}
