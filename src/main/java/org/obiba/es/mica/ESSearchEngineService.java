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

import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import jakarta.json.JsonObject;
import net.minidev.json.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.xcontent.XContentType;
import org.obiba.es.mica.mapping.DatasetIndexConfiguration;
import org.obiba.es.mica.mapping.FileIndexConfiguration;
import org.obiba.es.mica.mapping.NetworkIndexConfiguration;
import org.obiba.es.mica.mapping.PersonIndexConfiguration;
import org.obiba.es.mica.mapping.ProjectIndexConfiguration;
import org.obiba.es.mica.mapping.StudyIndexConfiguration;
import org.obiba.es.mica.mapping.TaxonomyIndexConfiguration;
import org.obiba.es.mica.mapping.VariableIndexConfiguration;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.obiba.mica.spi.search.Searcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ESSearchEngineService implements SearchEngineService {
  private final Logger log = LoggerFactory.getLogger(this.getClass());

  private static final String ES_BRANCH = "7.17.x";

  private static final String ES_CONFIG_FILE = "elasticsearch.yml";

  private Properties properties;

  private boolean running;

  private Node esNode;

  private ElasticsearchClient client;

  private ESIndexer esIndexer;

  private ESSearcher esSearcher;

  private ConfigurationProvider configurationProvider;

  private Set<Indexer.IndexConfigurationListener> indexConfigurationListeners;

  private String indexSettings;

  @Override
  public String getName() {
    return "mica-search-es";
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public void configure(Properties properties) {
    this.properties = properties;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void start() {
    // do init stuff
    if (properties != null) {
      Settings.Builder builder = getSettings();
      createTransportClient(builder);

      String bufferLimitBytes = builder.build().get("http.max_content_length_bytes");

      esIndexer = new ESIndexer(this);
      esSearcher = new ESSearcher(this, bufferLimitBytes == null || bufferLimitBytes.isEmpty() ? 250 * 1024 * 1024 : Integer.parseInt(bufferLimitBytes));

      running = true;
    }
  }

  @Override
  public void stop() {
    running = false;
    if (esNode != null) {
      try {
        esNode.close();
      } catch (IOException e) {
        log.error("Failed to close node {}", e);
      }
    }
    if (client != null) {
      client.shutdown();
    }
    esNode = null;
    client = null;
  }

  @Override
  public void setConfigurationProvider(ConfigurationProvider configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  public Indexer getIndexer() {
    return esIndexer;
  }

  @Override
  public Searcher getSearcher() {
    return esSearcher;
  }

  public ElasticsearchClient getClient() {
    return client;
  }

  ConfigurationProvider getConfigurationProvider() {
    return configurationProvider;
  }

  ObjectMapper getObjectMapper() {
    if (configurationProvider == null || configurationProvider.getObjectMapper() == null) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.findAndRegisterModules();
      return mapper;
    }
    return configurationProvider.getObjectMapper();
  }

  synchronized Set<Indexer.IndexConfigurationListener> getIndexConfigurationListeners() {
    if (indexConfigurationListeners == null) {
      indexConfigurationListeners = Sets.newHashSet();
      indexConfigurationListeners.add(new VariableIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new DatasetIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new StudyIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new NetworkIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new FileIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new PersonIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new ProjectIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new TaxonomyIndexConfiguration(configurationProvider));
    }
    return indexConfigurationListeners;
  }

  int getNbShards() {
    return Integer.parseInt(properties.getProperty("shards", "5"));
  }

  int getNbReplicas() {
    return Integer.parseInt(properties.getProperty("replicas", "1"));
  }

  //
  // Private methods
  //

  private void createTransportClient(Settings.Builder builder) {
    builder.put("client.transport.sniff", isTransportSniff());


    HttpHost[] httpHosts = getTransportAddresses()
      .stream()
      .map(transportAddress -> {
        int port = 9200;
        String host = transportAddress;
        int sepIdx = transportAddress.lastIndexOf(':');

        if (sepIdx > 0) {
          port = Integer.parseInt(transportAddress.substring(sepIdx + 1, transportAddress.length()));
          host = transportAddress.substring(0, sepIdx);
        }

        return new HttpHost(host, port, "http");
      })
      .toArray(HttpHost[]::new);

      RestClient restClient = RestClient.builder(httpHosts).build();
      RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

    client = new ElasticsearchClient(transport);
  }

  private boolean isDataNode() {
    return Boolean.parseBoolean(properties.getProperty("dataNode", "true"));
  }

  private String getClusterName() {
    return properties.getProperty("clusterName", "mica");
  }

  private List<String> getTransportAddresses() {
    String addStr = properties.getProperty("transportAddresses", "").trim();
    return addStr.isEmpty() ? Lists.newArrayList("localhost:9200") :
        Stream.of(addStr.split(",")).map(String::trim).collect(toList());
  }

  private boolean isTransportClient() {
    return Boolean.parseBoolean(properties.getProperty("transportClient", "false"));
  }

  private boolean isTransportSniff() {
    return Boolean.parseBoolean(properties.getProperty("transportSniff", "false"));
  }


  private File getWorkFolder() {
    return getServiceFolder(WORK_DIR_PROPERTY);
  }

  private File getInstallFolder() {
    return getServiceFolder(INSTALL_DIR_PROPERTY);
  }

  private File getServiceFolder(String dirProperty) {
    String defaultDir = new File(".").getAbsolutePath();
    String dataDirPath = properties.getProperty(dirProperty, defaultDir);
    File dataDir = new File(dataDirPath);
    if (!dataDir.exists()) dataDir.mkdirs();
    return dataDir;
  }

  String getIndexSettings() {
    return indexSettings;
  }

  private Settings.Builder getSettings() {
    File pluginWorkDir = new File(getWorkFolder(), properties.getProperty("es.version", ES_BRANCH));
    Settings.Builder builder = Settings.builder() //
        .put("path.home", getInstallFolder().getAbsolutePath()) //
        .put("path.data", new File(pluginWorkDir, "data").getAbsolutePath()) //
        .put("path.work", new File(pluginWorkDir, "work").getAbsolutePath());

    File defaultSettings = new File(getInstallFolder(), ES_CONFIG_FILE);
    if (defaultSettings.exists()) {
      try {
        builder.loadFromPath(defaultSettings.toPath());
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        Map map = objectMapper.readValue(defaultSettings, Map.class);
        JSONObject jsonObject = new JSONObject(map);
        JSONObject indexMap = new JSONObject((Map) jsonObject.get("index"));
        indexSettings = indexMap.toJSONString();
      } catch (IOException e) {
        log.error("Failed to load default settings {}", e);
      }
    }

    String settings = properties.getProperty("settings", "");
    if (!Strings.isNullOrEmpty(settings) && settings.indexOf(':') != -1) {
      builder.loadFromSource(settings, XContentType.YAML);
    }

    builder.put("cluster.name", getClusterName());

    return builder;
  }
}
