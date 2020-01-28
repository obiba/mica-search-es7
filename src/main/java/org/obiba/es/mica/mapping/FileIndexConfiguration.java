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

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FileIndexConfiguration extends AbstractIndexConfiguration {
  private static final Logger log = LoggerFactory.getLogger(FileIndexConfiguration.class);

  public FileIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    if (Indexer.ATTACHMENT_DRAFT_INDEX.equals(indexName) ||
        Indexer.ATTACHMENT_PUBLISHED_INDEX.equals(indexName)) {
      try {
        String attachmentField = Indexer.ATTACHMENT_DRAFT_INDEX.equals(indexName)
            ? "attachment"
            : "publishedAttachment";
        getClient(searchEngineService)
          .indices()
          .putMapping(new PutMappingRequest(indexName)
            .source(createMappingProperties(Indexer.ATTACHMENT_TYPE, attachmentField)), RequestOptions.DEFAULT);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private XContentBuilder createMappingProperties(String type, String attachmentField) throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
    mapping.startObject("properties");
    mapping.startObject("id").field("type", "keyword").endObject();
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    mapping.startObject("path").field("type", "keyword").endObject();
    mapping.startObject(attachmentField).startObject("properties");
    createMappingWithAndWithoutAnalyzer(mapping, "type");
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    createLocalizedMappingWithAnalyzers(mapping, "description");
    mapping.endObject().endObject(); // attachment
    mapping.endObject(); // properties
    mapping.endObject();

    return mapping;
  }
}
