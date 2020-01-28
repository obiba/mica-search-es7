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
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.obiba.mica.spi.search.Searcher;

/**
 * {@link Stats} aggregation wrapper.
 */
public class ESDocumentStatsAggregation implements Searcher.DocumentStatsAggregation {
  private final Stats stats;

  public ESDocumentStatsAggregation(Aggregation stats) {
    this.stats = (Stats) stats;
  }

  @Override
  public long getCount() {
    return stats.getCount();
  }

  @Override
  public double getMin() {
    return stats.getMin();
  }

  @Override
  public double getMax() {
    return stats.getMax();
  }

  @Override
  public double getAvg() {
    return stats.getAvg();
  }

  @Override
  public double getSum() {
    return stats.getSum();
  }
}
