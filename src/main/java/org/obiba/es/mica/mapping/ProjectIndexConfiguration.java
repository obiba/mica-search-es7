/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.mapping;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.indices.PutMappingRequest;

import java.io.IOException;
import java.io.StringReader;
import java.util.stream.Stream;

public class ProjectIndexConfiguration extends AbstractIndexConfiguration {
  private static final Logger log = LoggerFactory.getLogger(ProjectIndexConfiguration.class);

  public ProjectIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    if (Indexer.DRAFT_PROJECT_INDEX.equals(indexName) ||
        Indexer.PUBLISHED_PROJECT_INDEX.equals(indexName)) {

      try {
        XContentBuilder properties = createMappingProperties();

        getClient(searchEngineService)
          .indices()
          .putMapping(PutMappingRequest.of(r -> r.index(indexName).withJson(new StringReader(Strings.toString(properties)))));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private XContentBuilder createMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();

    // properties
    mapping.startObject("properties");
    mapping.startObject("id").field("type", "keyword").endObject();
    createMappingWithoutAnalyzer(mapping, "dataAccessRequestId");
    appendMembershipProperties(mapping);
    Stream.of(Indexer.PROJECT_LOCALIZED_ANALYZED_FIELDS)
        .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
    mapping.endObject();

    mapping.endObject();
    return mapping;
  }

}
