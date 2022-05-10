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
import co.elastic.clients.elasticsearch._types.aggregations.StatsAggregate;

import org.obiba.mica.spi.search.Searcher;

/**
 * {@link StatsAggregate} aggregation wrapper.
 */
public class ESDocumentStatsAggregation implements Searcher.DocumentStatsAggregation {
  private final StatsAggregate stats;

  public ESDocumentStatsAggregation(Aggregate stats) {
    this.stats = stats.stats();
  }

  @Override
  public long getCount() {
    return stats.count();
  }

  @Override
  public double getMin() {
    return stats.min();
  }

  @Override
  public double getMax() {
    return stats.max();
  }

  @Override
  public double getAvg() {
    return stats.avg();
  }

  @Override
  public double getSum() {
    return stats.sum();
  }
}
