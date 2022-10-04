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
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;

import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.obiba.mica.spi.search.Searcher;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link Terms} aggregation wrapper.
 */
public class ESDocumentTermsAggregation implements Searcher.DocumentTermsAggregation {
  private final StringTermsAggregate terms;

  public ESDocumentTermsAggregation(Aggregate terms) {
    this.terms = terms.sterms();
  }

  @Override
  public List<Searcher.DocumentTermsBucket> getBuckets() {
    
    return terms.buckets().array().stream().map(ESDocumentTermsBucket::new).collect(Collectors.toList());
  }
}
