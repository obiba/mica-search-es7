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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.obiba.es.mica.query.AndQuery;
import org.obiba.es.mica.query.RQLJoinQuery;
import org.obiba.es.mica.query.RQLQuery;
import org.obiba.es.mica.results.ESResponseCountResults;
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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.GlobalAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation.Builder.ContainerBuilder;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.core.search.SourceFilter;
import co.elastic.clients.elasticsearch.core.search.TrackHits;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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

  private final ObjectMapper objectMapper = new ObjectMapper();

  ESSearcher(ESSearchEngineService esSearchService) {
    this.esSearchService = esSearchService;

    RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
    builder.setHttpAsyncResponseConsumerFactory(
        new HttpAsyncResponseConsumerFactory
            .HeapBufferedResponseConsumerFactory(250 * 1024 * 1024));
  }

  ESSearcher(ESSearchEngineService esSearchService, int bufferLimitBytes) {
    this.esSearchService = esSearchService;

    RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
    builder.setHttpAsyncResponseConsumerFactory(
        new HttpAsyncResponseConsumerFactory
            .HeapBufferedResponseConsumerFactory(bufferLimitBytes));
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

    TrackHits trackHits = new TrackHits.Builder().enabled(true).build();
    Aggregation globalAggregation = new Aggregation.Builder().global(GlobalAggregation.of(agg -> agg.name(AGG_TOTAL_COUNT))).build();
    SourceConfig.Builder sourceConfigBuilder = new SourceConfig.Builder();

    if (AGGREGATION == scope) {
      sourceConfigBuilder.fetch(false);
    } else if (sourceFields != null) {
      if (sourceFields.isEmpty()) sourceConfigBuilder.fetch(false);
      else sourceConfigBuilder.filter(SourceFilter.of(s -> s.includes(sourceFields).excludes(null)));
    }

    Map<String, Aggregation> aggregations = new HashMap<>();
    aggregations.put(AGG_TOTAL_COUNT, globalAggregation);

    for (AbstractAggregationBuilder aggBuilder : getAggregations(query.getAggregationBuckets(), aggregationProperties)) {      
      aggregations.put(aggBuilder.getName(), new Aggregation.Builder().terms(agg -> agg.withJson(new StringReader(aggBuilder.toString()))).build());
    }

    co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

    SearchResponse<ObjectNode> response = getClient().search(s -> s.index(indexName)
      .query(esQuery)
      .from(query.getFrom())
      .size(scope == DETAIL ? query.getSize() : 0)
      .trackTotalHits(trackHits)
      .source(sourceConfigBuilder.build())
      .aggregations(aggregations), 
    ObjectNode.class);

    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.hits().total().value());

    return new ESResponseDocumentResults(response, objectMapper);
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
    SearchResponse<ObjectNode> response = null;

    try {
      TrackHits trackHits = new TrackHits.Builder().enabled(true).build(); 
      SourceConfig sourceConfig = new SourceConfig.Builder().fetch(false).build();
      Aggregation globalAggregation = new Aggregation.Builder().global(GlobalAggregation.of(agg -> agg.name(AGG_TOTAL_COUNT))).build();

      Map<String, Aggregation> aggregations = new HashMap<>();
      aggregations.put(AGG_TOTAL_COUNT, globalAggregation);

      for (AbstractAggregationBuilder aggBuilder : getAggregations(query.getAggregationBuckets(), aggregationProperties)) {      
        aggregations.put(aggBuilder.getName(), new Aggregation.Builder().terms(agg -> agg.withJson(new StringReader(aggBuilder.toString()))).build());
      }

      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .trackTotalHits(trackHits)
        .source(sourceConfig)
        .aggregations(aggregations), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to cover {} - {}", indexName, e);
    }

    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.hits().total().value());

    return new ESResponseDocumentResults(response, objectMapper);
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
    SearchResponse<ObjectNode> response = null;

    try {
      TrackHits trackHits = new TrackHits.Builder().enabled(true).build(); 
      SourceConfig sourceConfig = new SourceConfig.Builder().fetch(false).build();
      Aggregation globalAggregation = new Aggregation.Builder().global(GlobalAggregation.of(agg -> agg.name(AGG_TOTAL_COUNT))).build();

      Map<String, Aggregation> aggregations = new HashMap<>();
      aggregations.put(AGG_TOTAL_COUNT, globalAggregation);

      for (AbstractAggregationBuilder aggBuilder : aggregationParser.getAggregations(aggregationProperties, subAggregationProperties)) {
        aggregations.put(aggBuilder.getName(), new Aggregation.Builder().terms(agg -> agg.withJson(new StringReader(aggBuilder.toString()))).build());
      }

      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .trackTotalHits(trackHits)
        .source(sourceConfig)
        .aggregations(aggregations), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to cover {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.hits().total().value());

    return new ESResponseDocumentResults(response, objectMapper);
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
    SearchResponse<ObjectNode> response = null;
    try {
      TrackHits trackHits = new TrackHits.Builder().enabled(true).build(); 
      SourceConfig sourceConfig = new SourceConfig.Builder().fetch(false).build();
      Aggregation globalAggregation = new Aggregation.Builder().global(GlobalAggregation.of(agg -> agg.name(AGG_TOTAL_COUNT))).build();

      Map<String, Aggregation> aggregations = new HashMap<>();
      aggregations.put(AGG_TOTAL_COUNT, globalAggregation);

      for (AbstractAggregationBuilder aggBuilder : aggregationParser.getAggregations(aggregationProperties)) {
        aggregations.put(aggBuilder.getName(), new Aggregation.Builder().terms(agg -> agg.withJson(new StringReader(aggBuilder.toString()))).build());
      }

      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .trackTotalHits(trackHits)
        .source(sourceConfig)
        .aggregations(aggregations), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to aggregate {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.hits().total().value());

    return new ESResponseDocumentResults(response, objectMapper);
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
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      List<SortOptions> sortOptions = new ArrayList<>();

      if (query.hasSortBuilders()) {
        for (SortBuilder sortBuilder : query.getSortBuilders()) {
          sortOptions.add(new SortOptions.Builder().withJson(new StringReader(sortBuilder.toString())).build());
        }
      } else {
        sortOptions.add(new SortOptions.Builder().score(score -> score.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)).build());
      }     

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(query.getFrom())
        .size(query.getSize())
        .sort(sortOptions), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to find {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response, objectMapper);
  }

  @Override
  public DocumentResults count(String indexName, String type, String rql, IdFilter idFilter) {
    QueryBuilder filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    RQLQuery query = new RQLQuery(rql);

    List<String> aggregations = query.getAggregations();
    if (query.getAggregations() !=  null && !aggregations.isEmpty()) {
      return countWithAggregations(indexName, type, rql, idFilter);
    }

    QueryBuilder queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? QueryBuilders.matchAllQuery() : query.getQueryBuilder();
    QueryBuilder countQueryBuilder = filter == null ? queryBuilder : QueryBuilders.boolQuery().must(queryBuilder).must(filter);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, countQueryBuilder.toString());
    CountResponse response = null;
    try {
      response = getClient().count(r -> r.index(indexName).withJson(new StringReader(countQueryBuilder.toString())));
    } catch (IOException e) {
      log.error("Failed to count {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseCountResults(response);
  }

  /**
   * Client code does not require a total count but a count per aggregation.
   *
   * @param indexName
   * @param type
   * @param rql
   * @param idFilter
   * @return
   */
  private DocumentResults countWithAggregations(String indexName, String type, String rql, IdFilter idFilter) {
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
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      Map<String, Aggregation> aggregations = new HashMap<>();

      for (String field : query.getAggregations()) {
        aggregations.put(field, new Aggregation.Builder().terms(TermsAggregation.of(agg -> agg.field(field).size(Short.toUnsignedInt(Short.MAX_VALUE)))).build());
      }

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .aggregations(aggregations), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to count {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response, objectMapper);
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
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      SourceConfig sourceConfig = new SourceConfig.Builder().filter(SourceFilter.of(s -> s.includes(fieldName))).build();
      SortOptions sortOption = new SortOptions.Builder().score(score -> score.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)).build();

      SearchResponse<ObjectNode> response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(limit)
        .source(sourceConfig)
        .sort(sortOption), 
      ObjectNode.class);

      response.hits().hits().forEach(hit -> {
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
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to get document by ID {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    if (response == null || response.hits().total().value() == 0) return null;
    return new ByteArrayInputStream(response.hits().hits().get(0).toString().getBytes());
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
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to get document by class name {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    if (response == null || response.hits().total().value() == 0) return null;
    return new ByteArrayInputStream(response.hits().hits().get(0).toString().getBytes());
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
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      String capitalizedOrder = order.substring(0, 1).toUpperCase() + order.toLowerCase();

      SortOptions sortOption = sort != null ? 
        new SortOptions.Builder().field(FieldSort.of(s -> s.field(sort).order(order == null ? co.elastic.clients.elasticsearch._types.SortOrder.Asc : co.elastic.clients.elasticsearch._types.SortOrder.valueOf(capitalizedOrder)))).build() :
        new SortOptions.Builder().score(score -> score.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)).build();

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(from)
        .size(limit)
        .sort(sortOption), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to get documents by class name{} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response, objectMapper);
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
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      String capitalizedOrder = order.substring(0, 1).toUpperCase() + order.toLowerCase();

      SortOptions sortOption = sort != null ? 
        new SortOptions.Builder().field(FieldSort.of(s -> s.field(sort).order(order == null ? co.elastic.clients.elasticsearch._types.SortOrder.Asc : co.elastic.clients.elasticsearch._types.SortOrder.valueOf(capitalizedOrder)))).build() :
        new SortOptions.Builder().score(score -> score.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)).build();

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(from)
        .size(limit)
        .sort(sortOption), 
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to get documents {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response, objectMapper);
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

      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = co.elastic.clients.elasticsearch._types.query_dsl.Query.of(q -> q.withJson(new StringReader(sourceBuilder.query().toString())));

      Aggregation aggregation = new Aggregation.Builder().terms(TermsAggregation.of(agg -> agg.field(field).size(Short.toUnsignedInt(Short.MAX_VALUE)))).build();

      SearchResponse<ObjectNode> response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .aggregations(field.replaceAll("\\.", "-"), aggregation), 
      ObjectNode.class);

      log.debug("Response /{}/{}: {}", indexName, type, response);

      return response.aggregations().entrySet().stream().flatMap(a -> ((Terms) a).getBuckets().stream())
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

  private Iterable<AbstractAggregationBuilder> getAggregations(List<String> aggregationBuckets, Properties aggregationProperties) {
    Map<String, Properties> subAggregations = Maps.newHashMap();
    if (aggregationBuckets != null) aggregationBuckets.forEach(field -> subAggregations.put(field, aggregationProperties));
    aggregationParser.setLocales(esSearchService.getConfigurationProvider().getLocales());
    return aggregationParser.getAggregations(aggregationProperties, subAggregations);
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

  private ElasticsearchClient getClient() {
    return esSearchService.getClient();
  }

}
