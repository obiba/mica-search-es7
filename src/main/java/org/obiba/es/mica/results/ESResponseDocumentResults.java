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

import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.MultiTermsBucket;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import org.obiba.mica.spi.search.Searcher;
import org.obiba.mica.spi.search.Searcher.DocumentResult;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * {@link SearchResponse} wrapper.
 */
public class ESResponseDocumentResults implements Searcher.DocumentResults {
  private final SearchResponse<ObjectNode> response;
  private final ObjectMapper objectMapper;

  public ESResponseDocumentResults(SearchResponse<ObjectNode> response, ObjectMapper objectMapper) {
    this.response = response;
    this.objectMapper = objectMapper;
  }

  @Override
  public long getTotal() {
    return response.hits().total().value();
  }

  @Override
  public List<Searcher.DocumentResult> getDocuments() {
    return StreamSupport.stream(response.hits().hits().spliterator(), false)
        .map(h -> new ESHitDocumentResult(h, objectMapper))
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, Long> getAggregation(String field) {
    Aggregate aggregation = response.aggregations().get(field);
    return aggregation.multiTerms().buckets().array().stream().collect(Collectors.toMap(MultiTermsBucket::keyAsString, MultiTermsBucket::docCount));
  }

  @Override
  public List<Searcher.DocumentAggregation> getAggregations() {
    return response.aggregations().values().stream()
        .map(ESDocumentAggregation::new).collect(Collectors.toList());
  }
}
