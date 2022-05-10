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

import co.elastic.clients.elasticsearch.core.CountResponse;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.obiba.mica.spi.search.Searcher;

import java.util.List;
import java.util.Map;

public class ESResponseCountResults implements Searcher.DocumentResults {
  private final CountResponse response;

  public ESResponseCountResults(CountResponse response) {
    this.response = response;
  }

  @Override
  public long getTotal() {
    return response != null ? response.count() : 0;
  }

  @Override
  public List<Searcher.DocumentResult> getDocuments() {
    return Lists.newArrayList();
  }

  @Override
  public Map<String, Long> getAggregation(String field) {
    return Maps.newHashMap();
  }

  @Override
  public List<Searcher.DocumentAggregation> getAggregations() {
    return Lists.newArrayList();
  }
}
