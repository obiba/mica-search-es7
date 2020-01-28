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

import net.jazdw.rql.parser.ASTNode;
import net.jazdw.rql.parser.RQLParser;
import org.elasticsearch.common.Strings;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.IndexFieldMapping;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.rql.RQLFieldResolver;
import org.obiba.mica.spi.search.rql.RQLNode;
import org.obiba.mica.spi.search.support.EmptyQuery;
import org.obiba.mica.spi.search.support.JoinQuery;
import org.obiba.mica.spi.search.support.Query;
import org.obiba.opal.core.domain.taxonomy.Taxonomy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class RQLJoinQuery implements JoinQuery {

  private final ConfigurationProvider configurationProvider;

  private final Indexer indexer;

  private ASTNode node;

  private boolean withFacets;

  private String locale = DEFAULT_LOCALE;

  private Query variableQuery;

  private Query datasetQuery;

  private Query studyQuery;

  private Query networkQuery;

  private List<RQLNode> nodeTypes = new ArrayList<>();

  public RQLJoinQuery(ConfigurationProvider configurationProvider, Indexer indexer) {
    this.configurationProvider = configurationProvider;
    this.indexer = indexer;
  }

  @Override
  public boolean searchOnNetworksOnly() {
    return networkQuery.hasQueryBuilder()
            && !studyQuery.hasQueryBuilder()
            && !datasetQuery.hasQueryBuilder()
            && !variableQuery.hasQueryBuilder();
  }

  public void initialize(String rql) {
    String rqlStr = rql == null ? "" : rql;
    RQLParser parser = new RQLParser(new RQLConverter());
    node = parser.parse(rqlStr);
    initializeLocale(node);

    if (Strings.isNullOrEmpty(node.getName())) {
      List<ASTNode> mainNodes = node.getArguments().stream().filter(a -> a instanceof ASTNode).map(a -> (ASTNode) a).collect(toList());
      if (!containsVariableNode(mainNodes))
        mainNodes.add(new ASTNode("variable"));
      mainNodes.forEach(this::initialize);
    } else initialize(node);

    // make sure we have initialize everyone
    if (variableQuery == null) {
      variableQuery = new EmptyQuery();
    }
    if (datasetQuery == null) {
      datasetQuery = new EmptyQuery();
    }
    if (studyQuery == null) {
      studyQuery = new EmptyQuery();
    }
    if (networkQuery == null) {
      networkQuery = new EmptyQuery();
    }
  }

  private boolean containsVariableNode(List<ASTNode> mainNodes) {
    return mainNodes.stream()
            .anyMatch(node -> "variable".equals(node.getName()));
  }

  @Override
  public String getLocale() {
    return locale;
  }

  @Override
  public boolean isWithFacets() {
    return withFacets;
  }

  @Override
  public Query getVariableQuery() {
    return variableQuery;
  }

  @Override
  public Query getDatasetQuery() {
    return datasetQuery;
  }

  @Override
  public Query getStudyQuery() {
    return studyQuery;
  }

  @Override
  public Query getNetworkQuery() {
    return networkQuery;
  }

  //
  //
  //

  private void initialize(ASTNode node) {
    RQLNode rqlNode = RQLNode.valueOf(node.getName().toUpperCase());
    nodeTypes.add(rqlNode);
    switch (rqlNode) {
      case VARIABLE:
        variableQuery = new RQLQuery(node, new RQLFieldResolver(rqlNode, getVariableTaxonomies(), locale,
            getVariableIndexMapping()));
        break;
      case DATASET:
        datasetQuery = new RQLQuery(node, new RQLFieldResolver(rqlNode, getDatasetTaxonomies(), locale,
            getDatasetIndexMapping()));
        break;
      case STUDY:
        studyQuery = new RQLQuery(node, new RQLFieldResolver(rqlNode, getStudyTaxonomies(), locale,
            getStudyIndexMapping()));
        break;
      case NETWORK:
        networkQuery = new RQLQuery(node, new RQLFieldResolver(rqlNode, getNetworkTaxonomies(), locale,
            getNetworkIndexMapping()));
        break;
      case LOCALE:
        if (node.getArgumentsSize() > 0) locale = node.getArgument(0).toString();
        break;
      case FACET:
        withFacets = true;
        break;
    }
  }

  private void initializeLocale(ASTNode node) {
    if (Strings.isNullOrEmpty(node.getName())) {
      Optional<Object> localeNode = node.getArguments().stream()
          .filter(a -> a instanceof ASTNode && RQLNode.LOCALE == RQLNode.getType(((ASTNode) a).getName())).findFirst();

      localeNode.ifPresent(o -> initialize((ASTNode) o));
    } else if (RQLNode.LOCALE == RQLNode.getType(node.getName())) {
      initialize(node);
    }
  }

  private List<Taxonomy> getVariableTaxonomies() {
    return configurationProvider.getVariableTaxonomies();
  }

  private List<Taxonomy> getDatasetTaxonomies() {
    return Collections.singletonList(configurationProvider.getDatasetTaxonomy());
  }

  private List<Taxonomy> getStudyTaxonomies() {
    return Collections.singletonList(configurationProvider.getStudyTaxonomy());
  }

  private List<Taxonomy> getNetworkTaxonomies() {
    return Collections.singletonList(configurationProvider.getNetworkTaxonomy());
  }


  public IndexFieldMapping getVariableIndexMapping() {
    return getMapping(Indexer.PUBLISHED_VARIABLE_INDEX, Indexer.VARIABLE_TYPE);
  }

  public IndexFieldMapping getDatasetIndexMapping() {
    return getMapping(Indexer.DRAFT_DATASET_INDEX, Indexer.DATASET_TYPE);
  }

  public IndexFieldMapping getStudyIndexMapping() {
    return getMapping(Indexer.DRAFT_STUDY_INDEX, Indexer.STUDY_TYPE);
  }

  public IndexFieldMapping getNetworkIndexMapping() {
    return getMapping(Indexer.DRAFT_NETWORK_INDEX, Indexer.NETWORK_TYPE);
  }

  private IndexFieldMapping getMapping(String name, String type) {
    return indexer.getIndexfieldMapping(name, type);
  }
}
