/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.obiba.es.mica.query.AndQuery;
import org.obiba.es.mica.query.RQLJoinQuery;
import org.obiba.es.mica.query.RQLQuery;
import org.obiba.es.mica.results.ESResponseDocumentResults;
import org.obiba.es.mica.support.AggregationParser;
import org.obiba.es.mica.support.ESHitSourceMapHelper;
import org.obiba.mica.spi.search.QueryScope;
import org.obiba.mica.spi.search.Searcher;
import org.obiba.mica.spi.search.support.EmptyQuery;
import org.obiba.mica.spi.search.support.JoinQuery;
import org.obiba.mica.spi.search.support.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.obiba.mica.spi.search.QueryScope.AGGREGATION;
import static org.obiba.mica.spi.search.QueryScope.DETAIL;

public class ESSearcher implements Searcher {

  private static final Logger log = LoggerFactory.getLogger(Searcher.class);

  private final ESSearchEngineService esSearchService;

  private final AggregationParser aggregationParser = new AggregationParser();

  ESSearcher(ESSearchEngineService esSearchService) {
    this.esSearchService = esSearchService;
  }
  
  @Override
  public JoinQuery makeJoinQuery(String rql) {
    log.debug("makeJoinQuery: {}", rql);
    RQLJoinQuery joinQuery = new RQLJoinQuery(esSearchService.getConfigurationProvider(), esSearchService.getIndexer());
    joinQuery.initialize(rql);
    return joinQuery;
  }

  @Override
  public Query makeQuery(String rql) {
    log.debug("makeQuery: {}", rql);
    if (Strings.isNullOrEmpty(rql)) return new EmptyQuery();
    return new RQLQuery(rql);
  }

  @Override
  public Query andQuery(Query... queries) {
    return new AndQuery(queries);
  }

  @Override
  public DocumentResults query(String indexName, String type, Query query, QueryScope scope, List<String> mandatorySourceFields, Properties aggregationProperties, @Nullable IdFilter idFilter) throws IOException {

    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : ((ESQuery) query).getQueryBuilder();

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
      .query(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
      .from(query.getFrom()) //
      .size(scope == DETAIL ? query.getSize() : 0) //
      .trackTotalHits(true)
      .aggregation(AggregationBuilders.global(AGG_TOTAL_COUNT));

    List<String> sourceFields = getSourceFields(query, mandatorySourceFields);

    if (AGGREGATION == scope) {
      sourceBuilder.fetchSource(false);
    } else if (sourceFields != null) {
      if (sourceFields.isEmpty()) sourceBuilder.fetchSource(false);
      else sourceBuilder.fetchSource(sourceFields.toArray(new String[sourceFields.size()]), null);
    }

    if (!query.isEmpty()) ((ESQuery) query).getSortBuilders().forEach(sourceBuilder::sort);

    appendAggregations(sourceBuilder, query.getAggregationBuckets(), aggregationProperties);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = getClient().search(new SearchRequest(indexName).source(sourceBuilder).searchType(SearchType.QUERY_THEN_FETCH), RequestOptions.DEFAULT);
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.getHits().getTotalHits().value);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public DocumentResults cover(String indexName, String type, Query query, Properties aggregationProperties, @Nullable IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : ((ESQuery) query).getQueryBuilder();

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .from(0) //
        .size(0) // no results needed for a coverage
        .fetchSource(false)
        .trackTotalHits(true)
        .aggregation(AggregationBuilders.global(Searcher.AGG_TOTAL_COUNT));

    appendAggregations(sourceBuilder, query.getAggregationBuckets(), aggregationProperties);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;
    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder).searchType(SearchType.QUERY_THEN_FETCH), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to cover {} - {}", indexName, e);
    }

    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.getHits().getTotalHits().value);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public DocumentResults cover(String indexName, String type, Query query, Properties aggregationProperties, Map<String, Properties> subAggregationProperties, @Nullable IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : ((ESQuery) query).getQueryBuilder();


    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .from(0) //
        .size(0) // no results needed for a coverage
        .fetchSource(false)
        .trackTotalHits(true)
        .aggregation(AggregationBuilders.global(Searcher.AGG_TOTAL_COUNT));

    aggregationParser.getAggregations(aggregationProperties, subAggregationProperties).forEach(sourceBuilder::aggregation);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;

    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder).searchType(SearchType.QUERY_THEN_FETCH), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to cover {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.getHits().getTotalHits().value);

    return new ESResponseDocumentResults(response);
  }


  @Override
  public DocumentResults aggregate(String indexName, String type, Query query, Properties aggregationProperties, IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : ((ESQuery) query).getQueryBuilder();

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .from(0) //
        .size(0) // no results needed for a coverage
        .fetchSource(false)
        .trackTotalHits(true)
        .aggregation(AggregationBuilders.global(Searcher.AGG_TOTAL_COUNT));

    aggregationParser.getAggregations(aggregationProperties).forEach(sourceBuilder::aggregation);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;
    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder).searchType(SearchType.QUERY_THEN_FETCH), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to aggregate {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.getHits().getTotalHits().value);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public DocumentResults find(String indexName, String type, String rql, IdFilter idFilter) {

    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);

    RQLQuery query = new RQLQuery(rql);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : query.getQueryBuilder();

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter))
        .from(query.getFrom())
        .size(query.getSize());

    if (query.hasSortBuilders())
      query.getSortBuilders().forEach(sourceBuilder::sort);
    else
      sourceBuilder.sort(SortBuilders.scoreSort().order(SortOrder.DESC));

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;
    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder).searchType(SearchType.QUERY_THEN_FETCH), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to find {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public DocumentResults count(String indexName, String type, String rql, IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    RQLQuery query = new RQLQuery(rql);
    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : query.getQueryBuilder();

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter)) //
        .from(0)
        .size(0);

    query.getAggregations().forEach(field -> sourceBuilder.aggregation(AggregationBuilders.terms(field).field(field).size(Short.MAX_VALUE)));

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;
    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to count {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public List<String> suggest(String indexName, String type, int limit, String locale, String queryString, String defaultFieldNamePattern) {
    String localizedFieldName = String.format(defaultFieldNamePattern, locale);
    String fieldName = localizedFieldName.replace(".analyzed", "");

    QueryBuilder queryExec = QueryBuilders.queryStringQuery(queryString)
        .defaultField(localizedFieldName)
        .defaultOperator(Operator.OR);

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(queryExec) //
        .from(0) //
        .size(limit)
        .sort(SortBuilders.scoreSort().order(SortOrder.DESC))
        .fetchSource(new String[]{fieldName}, null);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    List<String> names = Lists.newArrayList();

    try {
      SearchResponse response = getClient().search(new SearchRequest(indexName).source(sourceBuilder), RequestOptions.DEFAULT);
      response.getHits().forEach(hit -> {
          String value = ESHitSourceMapHelper.flattenMap(hit).get(fieldName).toLowerCase();
          names.add(Joiner.on(" ").join(Splitter.on(" ").trimResults().splitToList(value).stream()
            .filter(str -> !str.contains("[") && !str.contains("(") && !str.contains("{") && !str.contains("]") && !str.contains(")") && !str.contains("}"))
            .map(str -> str.replace(":", "").replace(",", ""))
            .filter(str -> !str.isEmpty()).collect(Collectors.toList())));
        }
      );

    } catch (IOException e) {
      log.error("Failed to suggest {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);


    return names;
  }

  @Override
  public InputStream getDocumentById(String indexName, String type, String id) {
    QueryBuilder query = new IdsQueryBuilder().addIds(id);

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(query);

    log.debug("Request: /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;
    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to get document by ID {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    if (response == null || response.getHits().getTotalHits().value == 0) return null;
    return new ByteArrayInputStream(response.getHits().getAt(0).getSourceAsString().getBytes());
  }

  @Override
  public InputStream getDocumentByClassName(String indexName, String type, Class clazz, String id) {
    QueryBuilder query = QueryBuilders.queryStringQuery(clazz.getSimpleName()).field("className");
    query = QueryBuilders.boolQuery().must(query)
        .must(QueryBuilders.idsQuery().addIds(id));

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(query);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;
    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to get document by class name {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    if (response == null || response.getHits().getTotalHits().value == 0) return null;
    return new ByteArrayInputStream(response.getHits().getAt(0).getSourceAsString().getBytes());
  }

  @Override
  public DocumentResults getDocumentsByClassName(String indexName, String type, Class clazz, int from, int limit,
                                                 String sort, String order, String queryString,
                                                 TermFilter termFilter, IdFilter idFilter) {

    QueryBuilder query = QueryBuilders.queryStringQuery(clazz.getSimpleName()).field("className");
    if (queryString != null) {
      query = QueryBuilders.boolQuery().must(query).must(QueryBuilders.queryStringQuery(queryString));
    }

    QueryBuilder postFilter = getPostFilter(termFilter, idFilter);

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(postFilter == null ? query : QueryBuilders.boolQuery().must(query).must(postFilter)) //
        .from(from) //
        .size(limit);

    if (sort != null) {
      sourceBuilder.sort(
          SortBuilders.fieldSort(sort).order(order == null ? SortOrder.ASC : SortOrder.valueOf(order.toUpperCase())));
    }

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;
    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to get documents by class name{} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public DocumentResults getDocuments(String indexName, String type, int from, int limit, @Nullable String sort, @Nullable String order, @Nullable String queryString, @Nullable TermFilter termFilter, @Nullable IdFilter idFilter, @Nullable List<String> fields, @Nullable List<String> excludedFields) {
    QueryStringQueryBuilder query = queryString != null ? QueryBuilders.queryStringQuery(queryString) : null;

    if (query != null && fields != null) fields.forEach(query::field);

    QueryBuilder postFilter = getPostFilter(termFilter, idFilter);

    QueryBuilder execQuery = postFilter == null ? query : query == null ? postFilter : QueryBuilders.boolQuery().must(query).filter(postFilter);

    if (excludedFields != null) {
      BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
      excludedFields.forEach(f -> boolQueryBuilder.mustNot(
          QueryBuilders.boolQuery().must(QueryBuilders.termQuery(f, "true")).must(QueryBuilders.existsQuery(f))));
      execQuery = boolQueryBuilder.must(execQuery);
    }

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(execQuery) //
        .from(from) //
        .size(limit);

    if (sort != null) {
      sourceBuilder.sort(
          SortBuilders.fieldSort(sort).order(order == null ? SortOrder.ASC : SortOrder.valueOf(order.toUpperCase())));
    }

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
    SearchResponse response = null;
    try {
      response = getClient().search(new SearchRequest(indexName).source(sourceBuilder), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to get documents {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response);
  }

  @Override
  public long countDocumentsWithField(String indexName, String type, String field) {
    BoolQueryBuilder builder = QueryBuilders.boolQuery()
        .should(QueryBuilders.existsQuery(field));

    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
        .query(builder)
        .from(0) //
        .size(0)
        .aggregation(AggregationBuilders.terms(field.replaceAll("\\.", "-")).field(field).size(Short.MAX_VALUE));

    try {
      log.debug("Request /{}/{}: {}", indexName, type, sourceBuilder);
      if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, sourceBuilder.toString());
      SearchResponse response = getClient().search(new SearchRequest(indexName).source(sourceBuilder).searchType(SearchType.QUERY_THEN_FETCH), RequestOptions.DEFAULT);
      log.debug("Response /{}/{}: {}", indexName, type, response);

      return response.getAggregations().asList().stream().flatMap(a -> ((Terms) a).getBuckets().stream())
          .map(a -> a.getKey().toString()).distinct().collect(Collectors.toList()).size();
    } catch (IndexNotFoundException | IOException e) {
      log.warn("Count of Studies With Variables failed", e);
      return 0;
    }
  }

  //
  // Private methods
  //

  private QueryBuilder getPostFilter(TermFilter termFilter, IdFilter idFilter) {
    QueryBuilder filter = null;

    if (idFilter != null)
      filter = getIdQueryBuilder(idFilter);

    if (termFilter != null && termFilter.getValue() != null) {
      QueryBuilder filterBy = QueryBuilders.termQuery(termFilter.getField(), termFilter.getValue());
      filter = filter == null ? filterBy : QueryBuilders.boolQuery().must(filter).must(filterBy);
    }

    return filter;
  }

  private QueryBuilder getIdQueryBuilder(IdFilter idFilter) {
    if (idFilter instanceof PathFilter) return getPathQueryBuilder((PathFilter) idFilter);
    QueryBuilder filter;
    Collection<String> ids = idFilter.getValues();
    if (ids.isEmpty())
      filter = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("id"));
    else if ("id".equals(idFilter.getField()))
      filter = QueryBuilders.idsQuery().addIds(ids.toArray(new String[]{}));
    else {
      BoolQueryBuilder orFilter = QueryBuilders.boolQuery();
      ids.forEach(id -> orFilter.should(QueryBuilders.termQuery(idFilter.getField(), id)));
      filter = orFilter;
      // FIXME filter = QueryBuilders.termsQuery(idFilter.getField(), ids);
    }
    return filter;
  }

  private QueryBuilder getPathQueryBuilder(PathFilter pathFilter) {
    List<QueryBuilder> includes = pathFilter.getValues().stream()
        .map(path -> path.endsWith("/") ? QueryBuilders.prefixQuery(pathFilter.getField(), path)
            : QueryBuilders.termQuery(pathFilter.getField(), path))
        .collect(Collectors.toList());
    List<QueryBuilder> excludes = pathFilter.getExcludedValues().stream()
        .map(path -> QueryBuilders.prefixQuery(pathFilter.getField(), path))
        .collect(Collectors.toList());

    BoolQueryBuilder includedFilter = QueryBuilders.boolQuery();
    includes.forEach(includedFilter::should);
    if (excludes.isEmpty()) return includedFilter;

    BoolQueryBuilder excludedFilter = QueryBuilders.boolQuery();
    excludes.forEach(excludedFilter::should);

    return QueryBuilders.boolQuery().must(includedFilter).mustNot(excludedFilter);
  }

  private void appendAggregations(SearchSourceBuilder requestBuilder, List<String> aggregationBuckets, Properties aggregationProperties) {
    Map<String, Properties> subAggregations = Maps.newHashMap();
    if (aggregationBuckets != null)
      aggregationBuckets.forEach(field -> subAggregations.put(field, aggregationProperties));
    aggregationParser.setLocales(esSearchService.getConfigurationProvider().getLocales());
    aggregationParser.getAggregations(aggregationProperties, subAggregations).forEach(requestBuilder::aggregation);
  }

  /**
   * Returns the default source filtering fields. A NULL signifies the whole source to be included
   */
  private List<String> getSourceFields(Query query, List<String> mandatorySourceFields) {
    List<String> sourceFields = query.getSourceFields();

    if (sourceFields != null && !sourceFields.isEmpty()) {
      sourceFields.addAll(mandatorySourceFields);
    }

    return sourceFields;
  }

  private RestHighLevelClient getClient() {
    return esSearchService.getClient();
  }

}
