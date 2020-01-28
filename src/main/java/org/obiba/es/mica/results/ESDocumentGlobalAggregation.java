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
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.obiba.mica.spi.search.Searcher;

/**
 * {@link Global} aggregation wrapper.
 */
public class ESDocumentGlobalAggregation implements Searcher.DocumentGlobalAggregation {
  private final Global global;

  public ESDocumentGlobalAggregation(Aggregation global) {
    this.global = (Global) global;
  }

  @Override
  public long getDocCount() {
    return global.getDocCount();
  }
}
