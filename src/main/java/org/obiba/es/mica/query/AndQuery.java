/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.obiba.es.mica.ESQuery;
import org.obiba.mica.spi.search.support.Query;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Combines several queries with and/must.
 */
public class AndQuery implements ESQuery {

  private final List<ESQuery> queries;

  public AndQuery(Query... queries) {
    this.queries = Lists.newArrayList(queries).stream().filter(q -> !q.isEmpty())
        .map(q -> (ESQuery) q).collect(Collectors.toList());
  }

  @Override
  public int getFrom() {
    return queries.stream().filter(ESQuery::hasLimit).map(ESQuery::getFrom).findFirst().orElse(0);
  }

  @Override
  public int getSize() {
    return queries.stream().filter(ESQuery::hasLimit).map(ESQuery::getSize).findFirst().orElse(0);
  }

  @Override
  public boolean hasLimit() {
    return queries.stream().anyMatch(ESQuery::hasLimit);
  }

  @Override
  public boolean hasQueryBuilder() {
    return queries.stream().anyMatch(ESQuery::hasQueryBuilder);
  }

  @Override
  public QueryBuilder getQueryBuilder() {
    BoolQueryBuilder builder = QueryBuilders.boolQuery();
    queries.stream().filter(ESQuery::hasQueryBuilder).forEach(q -> builder.must(q.getQueryBuilder()));
    return builder;
  }

  @Override
  public boolean hasSortBuilders() {
    return queries.stream().anyMatch(ESQuery::hasSortBuilders);
  }

  @Override
  public List<SortBuilder> getSortBuilders() {
    List<SortBuilder> sorts = Lists.newArrayList();
    queries.stream().filter(ESQuery::hasSortBuilders).forEach(q -> sorts.addAll(q.getSortBuilders()));
    return sorts;
  }

  @Override
  public List<String> getSourceFields() {
    List<String> fields = Lists.newArrayList();
    queries.forEach(q -> fields.addAll(q.getSourceFields()));
    return fields;
  }

  @Override
  public List<String> getAggregationBuckets() {
    List<String> buckets = Lists.newArrayList();
    queries.forEach(q -> buckets.addAll(q.getAggregationBuckets()));
    return buckets;
  }

  @Override
  public List<String> getQueryAggregationBuckets() {
    return Lists.newArrayList();
  }

  @Override
  public void ensureAggregationBuckets(List<String> additionalAggregationBuckets) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAggregations() {
    List<String> aggs = Lists.newArrayList();
    queries.forEach(q -> aggs.addAll(q.getAggregations()));
    return aggs;
  }

  @Override
  public Map<String, Map<String, List<String>>> getTaxonomyTermsMap() {
    return Maps.newHashMap();
  }

  @Override
  public boolean isEmpty() {
    return queries.stream().allMatch(Query::isEmpty);
  }

  @Override
  public boolean hasIdCriteria() {
    return queries.stream().anyMatch(Query::hasIdCriteria);
  }
}
