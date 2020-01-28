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
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.obiba.mica.spi.search.Searcher;

/**
 * {@link Aggregation} wrapper.
 */
public class ESDocumentAggregation implements Searcher.DocumentAggregation {
  private final Aggregation aggregation;

  public ESDocumentAggregation(Aggregation aggregation) {
    this.aggregation = aggregation;
  }

  @Override
  public String getName() {
    return aggregation.getName();
  }

  @Override
  public String getType() {
    return aggregation.getType();
  }

  @Override
  public Searcher.DocumentStatsAggregation asStats() {
    return new ESDocumentStatsAggregation(aggregation);
  }

  @Override
  public Searcher.DocumentTermsAggregation asTerms() {
    return new ESDocumentTermsAggregation(aggregation);
  }

  @Override
  public Searcher.DocumentRangeAggregation asRange() {
    return new ESDocumentRangeAggregation(aggregation);
  }

  @Override
  public Searcher.DocumentGlobalAggregation asGlobal() {
    return new ESDocumentGlobalAggregation(aggregation);
  }
}
