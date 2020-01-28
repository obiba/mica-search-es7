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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.obiba.mica.spi.search.Searcher;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * {@link SearchResponse} wrapper.
 */
public class ESResponseDocumentResults implements Searcher.DocumentResults {
  private final SearchResponse response;

  public ESResponseDocumentResults(SearchResponse response) {
    this.response = response;
  }

  @Override
  public long getTotal() {
    return response.getHits().getTotalHits().value;
  }

  @Override
  public List<Searcher.DocumentResult> getDocuments() {
    return StreamSupport.stream(response.getHits().spliterator(), false)
        .map(ESHitDocumentResult::new)
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, Long> getAggregation(String field) {
    Terms aggregation = response.getAggregations().get(field);
    return aggregation.getBuckets().stream().collect(Collectors.toMap(MultiBucketsAggregation.Bucket::getKeyAsString, MultiBucketsAggregation.Bucket::getDocCount));
  }

  @Override
  public List<Searcher.DocumentAggregation> getAggregations() {
    return response.getAggregations().asList().stream()
        .map(ESDocumentAggregation::new).collect(Collectors.toList());
  }
}
