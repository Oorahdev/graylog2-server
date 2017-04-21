/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.indexer.indices;

import com.github.joschi.jadconfig.util.Duration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.State;
import io.searchbox.indices.CloseIndex;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.Flush;
import io.searchbox.indices.ForceMerge;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.OpenIndex;
import io.searchbox.indices.Stats;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.AliasExists;
import io.searchbox.indices.aliases.AliasMapping;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.aliases.RemoveAliasMapping;
import io.searchbox.indices.settings.GetSettings;
import io.searchbox.indices.settings.UpdateSettings;
import io.searchbox.indices.template.DeleteTemplate;
import io.searchbox.indices.template.PutTemplate;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortParseElement;
import org.graylog2.audit.AuditActor;
import org.graylog2.audit.AuditEventSender;
import org.graylog2.indexer.IndexMapping;
import org.graylog2.indexer.IndexNotFoundException;
import org.graylog2.indexer.IndexSet;
import org.graylog2.indexer.indexset.IndexSetConfig;
import org.graylog2.indexer.messages.Messages;
import org.graylog2.indexer.searches.IndexRangeStats;
import org.graylog2.plugin.system.NodeId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.graylog2.audit.AuditEventTypes.ES_INDEX_CREATE;

@Singleton
public class Indices {
    private static final Logger LOG = LoggerFactory.getLogger(Indices.class);
    private static final String REOPENED_INDEX_SETTING = "graylog2_reopened";

    private final Client c;
    private final JestClient jestClient;
    private final IndexMapping indexMapping;
    private final Messages messages;
    private final NodeId nodeId;
    private final AuditEventSender auditEventSender;

    @Inject
    public Indices(Client client, JestClient jestClient, IndexMapping indexMapping, Messages messages, NodeId nodeId, AuditEventSender auditEventSender) {
        this.c = client;
        this.jestClient = jestClient;
        this.indexMapping = indexMapping;
        this.messages = messages;
        this.nodeId = nodeId;
        this.auditEventSender = auditEventSender;
    }

    public void move(String source, String target) {
        SearchResponse scrollResp = c.prepareSearch(source)
                .setScroll(TimeValue.timeValueSeconds(10L))
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort(SortParseElement.DOC_FIELD_NAME))
                .setSize(350)
                .execute()
                .actionGet();

        while (true) {
            scrollResp = c.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

            // No more hits.
            if (scrollResp.getHits().hits().length == 0) {
                break;
            }

            final BulkRequestBuilder request = c.prepareBulk();
            for (SearchHit hit : scrollResp.getHits()) {
                Map<String, Object> doc = hit.getSource();
                String id = (String) doc.remove("_id");

                request.add(messages.buildIndexRequest(target, doc, id));
            }

            request.setConsistencyLevel(WriteConsistencyLevel.ONE);

            if (request.numberOfActions() > 0) {
                BulkResponse response = c.bulk(request.request()).actionGet();

                LOG.info("Moving index <{}> to <{}>: Bulk indexed {} messages, took {} ms, failures: {}",
                        source,
                        target,
                        response.getItems().length,
                        response.getTookInMillis(),
                        response.hasFailures());

                if (response.hasFailures()) {
                    throw new RuntimeException("Failed to move a message. Check your indexer log.");
                }
            }
        }

    }

    public void delete(String indexName) {
        try {
            jestClient.execute(new DeleteIndex.Builder(indexName).build());
        } catch (IOException e) {
            LOG.info("Couldn't delete index {}", indexName, e);
        }
    }

    public void close(String indexName) {
        try {
            jestClient.execute(new CloseIndex.Builder(indexName).build());
        } catch (IOException e) {
            LOG.info("Couldn't close index {}", indexName, e);
        }
    }

    public long numberOfMessages(String indexName) throws IndexNotFoundException {
        final IndexStats index = indexStats(indexName);
        if (index == null) {
            throw new IndexNotFoundException("Couldn't find index " + indexName);
        }

        final DocsStats docsStats = index.getPrimaries().getDocs();

        return docsStats == null ? 0L : docsStats.getCount();
    }

    public Map<String, JsonObject> getAll(final IndexSet indexSet) {
        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(new Stats.Builder().addIndex(indexSet.getIndexWildcard()).build());
        } catch (IOException e) {
            LOG.info("Couldn't fetch index stats of {}", indexSet, e);
            return Collections.emptyMap();
        }

        if (!jestResult.isSucceeded()) {
            LOG.info("Couldn't fetch index stats of {}: {}", indexSet, jestResult.getErrorMessage());
            return Collections.emptyMap();
        }

        final JsonObject responseJson = jestResult.getJsonObject();
        final JsonObject indices = responseJson.getAsJsonObject("indices");
        final JsonObject shards = responseJson.getAsJsonObject("_shards");
        final int failedShards = shards.get("failed").getAsInt();

        if (failedShards > 0) {
            LOG.warn("Index stats response contains failed shards, response is incomplete");
        }

        final ImmutableMap.Builder<String, JsonObject> indexStatsBuilder = ImmutableMap.builder();
        for (Map.Entry<String, JsonElement> entry : indices.entrySet()) {
            if (entry.getValue().isJsonObject()) {
                indexStatsBuilder.put(entry.getKey(), entry.getValue().getAsJsonObject());
            }
        }

        return indexStatsBuilder.build();
    }

    public Map<String, IndexStats> getAllDocCounts(final IndexSet indexSet) {
        final IndicesStatsRequest request = c.admin().indices().prepareStats(indexSet.getIndexWildcard()).setDocs(true).request();
        final IndicesStatsResponse response = c.admin().indices().stats(request).actionGet();

        return response.getIndices();
    }

    @Nullable
    private IndexStats indexStats(final String indexName) {
        final IndicesStatsRequest request = c.admin().indices().prepareStats(indexName).request();
        final IndicesStatsResponse response = c.admin().indices().stats(request).actionGet();

        return response.getIndex(indexName);
    }

    public boolean exists(String indexName) {
        try {
            return jestClient.execute(new IndicesExists.Builder(indexName).build()).isSucceeded();
        } catch (IOException e) {
            LOG.info("Couldn't check existence of index {}", indexName, e);
            return false;
        }
    }

    public boolean aliasExists(String alias) {
        try {
            return jestClient.execute(new AliasExists.Builder().alias(alias).build()).isSucceeded();
        } catch (IOException e) {
            LOG.info("Couldn't check existence of alias {}", alias, e);
            return false;
        }
    }

    @NotNull
    public Map<String, Set<String>> getIndexNamesAndAliases(String indexPattern) {

        // only request indices matching the name or pattern in `indexPattern` and only get the alias names for each index,
        // not the settings or mappings
        final GetIndexRequestBuilder getIndexRequestBuilder = c.admin().indices().prepareGetIndex();
        getIndexRequestBuilder.addFeatures(GetIndexRequest.Feature.ALIASES);
        getIndexRequestBuilder.setIndices(indexPattern);

        final GetIndexResponse getIndexResponse = c.admin().indices().getIndex(getIndexRequestBuilder.request()).actionGet();

        final String[] indices = getIndexResponse.indices();
        final ImmutableOpenMap<String, List<AliasMetaData>> aliases = getIndexResponse.aliases();
        final Map<String, Set<String>> indexAliases = Maps.newHashMap();
        for (String index : indices) {
            final List<AliasMetaData> aliasMetaData = aliases.get(index);
            if (aliasMetaData == null) {
                indexAliases.put(index, Collections.emptySet());
            } else {
                indexAliases.put(index,
                                 aliasMetaData.stream()
                                         .map(AliasMetaData::alias)
                                         .collect(toSet()));
            }
        }

        return indexAliases;
    }

    @Nullable
    public String aliasTarget(String alias) throws TooManyAliasesException {
        final IndicesAdminClient indicesAdminClient = c.admin().indices();

        final GetAliasesRequest request = indicesAdminClient.prepareGetAliases(alias).request();
        final GetAliasesResponse response = indicesAdminClient.getAliases(request).actionGet();

        // The ES return value of this has an awkward format: The first key of the hash is the target index. Thanks.
        final ImmutableOpenMap<String, List<AliasMetaData>> aliases = response.getAliases();

        if (aliases.size() > 1) {
            throw new TooManyAliasesException(Sets.newHashSet(aliases.keysIt()));
        }
        return aliases.isEmpty() ? null : aliases.keysIt().next();
    }

    private void ensureIndexTemplate(IndexSet indexSet) {
        final IndexSetConfig indexSetConfig = indexSet.getConfig();
        final String templateName = indexSetConfig.indexTemplateName();
        final Map<String, Object> template = indexMapping.messageTemplate(indexSet.getIndexWildcard(), indexSetConfig.indexAnalyzer(), -1);

        final PutTemplate request = new PutTemplate.Builder(templateName, template).build();

        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(request);
        } catch (IOException e) {
            LOG.error("Unable to create index template {}", templateName, e);
            return;
        }

        if (jestResult.isSucceeded()) {
            LOG.info("Successfully created index template {}", templateName);
        } else {
            LOG.error("Unable to create index template {}: {}" + templateName, jestResult.getErrorMessage());
        }
    }

    public void deleteIndexTemplate(IndexSet indexSet) {
        final String templateName = indexSet.getConfig().indexTemplateName();
        try {
            final JestResult result = jestClient.execute(new DeleteTemplate.Builder(templateName).build());
            if (result.isSucceeded()) {
                LOG.info("Successfully deleted index template {}", templateName);
            } else {
                LOG.error("Unable to delete the index template {}: ", templateName, result.getErrorMessage());
            }
        } catch (IOException e) {
            LOG.error("Unable to delete the Graylog index template {}", templateName, e);
        }
    }

    public boolean create(String indexName, IndexSet indexSet) {
        return create(indexName, indexSet, Collections.emptyMap());
    }

    public boolean create(String indexName, IndexSet indexSet, Map<String, Object> customSettings) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards", indexSet.getConfig().shards());
        settings.put("number_of_replicas", indexSet.getConfig().replicas());
        settings.putAll(customSettings);

        final CreateIndex request = new CreateIndex.Builder(indexName)
                .settings(settings)
                .build();

        // Make sure our index template exists before creating an index!
        ensureIndexTemplate(indexSet);

        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(request);
        } catch (IOException e) {
            LOG.warn("Couldn't create index {}", indexName, e);
            return false;
        }

        final boolean succeeded = jestResult.isSucceeded();
        if (succeeded) {
            auditEventSender.success(AuditActor.system(nodeId), ES_INDEX_CREATE, ImmutableMap.of("indexName", indexName));
        } else {
            LOG.warn("Couldn't create index {}: {}", indexName, jestResult.getErrorMessage());
            auditEventSender.failure(AuditActor.system(nodeId), ES_INDEX_CREATE, ImmutableMap.of("indexName", indexName));
        }
        return succeeded;
    }

    public Map<String, Set<String>> getAllMessageFieldsForIndices(final String[] writeIndexWildcards) {
        final String indices = String.join(",", writeIndexWildcards);
        final State request = new State.Builder()
                .indices(indices)
                .withMetadata()
                .build();

        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(request);
        } catch (IOException e) {
            LOG.warn("Couldn't read cluster state for indices {}", writeIndexWildcards, e);
            return Collections.emptyMap();
        }

        // The calls to JsonElement::isJsonObject are necessary because JsonElement::getAsJsonObject
        // will throw an IllegalStateException if the JsonElement is no JsonObject.
        final JsonObject indicesJson = getClusterStateIndicesMetadata(jestResult.getJsonObject());

        final ImmutableMap.Builder<String, Set<String>> fields = ImmutableMap.builder();
        for (Map.Entry<String, JsonElement> entry : indicesJson.entrySet()) {
            final String indexName = entry.getKey();
            final Set<String> fieldNames = Optional.ofNullable(entry.getValue())
                    .filter(JsonElement::isJsonObject)
                    .map(JsonElement::getAsJsonObject)
                    .map(index -> index.get("mappings"))
                    .filter(JsonElement::isJsonObject)
                    .map(JsonElement::getAsJsonObject)
                    .map(mappings -> mappings.get(IndexMapping.TYPE_MESSAGE))
                    .filter(JsonElement::isJsonObject)
                    .map(JsonElement::getAsJsonObject)
                    .map(messageType -> messageType.get("properties"))
                    .filter(JsonElement::isJsonObject)
                    .map(JsonElement::getAsJsonObject)
                    .map(JsonObject::entrySet)
                    .map(Set::stream)
                    .orElseGet(Stream::empty)
                    .map(Map.Entry::getKey)
                    .collect(toSet());

            if (!fieldNames.isEmpty()) {
                fields.put(indexName, fieldNames);
            }
        }

        return fields.build();
    }

    public Set<String> getAllMessageFields(final String[] writeIndexWildcards) {
        return  getAllMessageFieldsForIndices(writeIndexWildcards)
            .values()
            .stream()
            .reduce((strings, strings2) -> ImmutableSet.<String>builder().addAll(strings).addAll(strings2).build())
            .orElse(Collections.emptySet());
    }

    public void setReadOnly(String index) {
        // https://www.elastic.co/guide/en/elasticsearch/reference/2.4/indices-update-settings.html
        final Map<String, Object> settings = ImmutableMap.of(
                "index", ImmutableMap.of("blocks",
                        ImmutableMap.of(
                                "write", true, // Block writing.
                                "read", false, // Allow reading.
                                "metadata", false) // Allow getting metadata.
                )
        );

        final UpdateSettings request = new UpdateSettings.Builder(settings).addIndex(index).build();
        try {
            jestClient.execute(request);
        } catch (IOException e) {
            LOG.warn("Couldn't set index {} to read-only", index, e);
        }
    }

    public void flush(String index) {
        try {
            jestClient.execute(new Flush.Builder().addIndex(index).force().build());
        } catch (IOException e) {
            LOG.warn("Couldn't flush index {}", index, e);
        }
    }

    public void reopenIndex(String index) {
        // Mark this index as re-opened. It will never be touched by retention.
        final Map<String, Object> settings = ImmutableMap.of("index", ImmutableMap.of(REOPENED_INDEX_SETTING, true));
        final UpdateSettings request = new UpdateSettings.Builder(settings).addIndex(index).build();
        try {
            final JestResult jestResult = jestClient.execute(request);
            if (!jestResult.isSucceeded()) {
                LOG.warn("Couldn't update settings of index {}: {}", index, jestResult.getErrorMessage());
            }
        } catch (IOException e) {
            LOG.warn("Couldn't update settings of index {}", index, e);
        }


        // Open index.
        openIndex(index);
    }

    private void openIndex(String index) {
        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(new OpenIndex.Builder(index).build());
            if (!jestResult.isSucceeded()) {
                LOG.error("Couldn't open index {}: {}", index, jestResult.getErrorMessage());
            }
        } catch (IOException e) {
            LOG.error("Couldn't open index {}", index, e);
        }
    }

    public boolean isReopened(String indexName) {
        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(new State.Builder().withMetadata().build());
        } catch (IOException e) {
            LOG.warn("Couldn't read cluster state for index {}", indexName, e);
            return false;
        }

        // The calls to JsonElement::isJsonObject are necessary because JsonElement::getAsJsonObject
        // will throw an IllegalStateException if the JsonElement is no JsonObject.
        final JsonObject indexJson = Optional.ofNullable(jestResult.getJsonObject())
                .map(response -> response.get("metadata"))
                .filter(JsonElement::isJsonObject)
                .map(JsonElement::getAsJsonObject)
                .map(metadata -> metadata.get("indices"))
                .filter(JsonElement::isJsonObject)
                .map(JsonElement::getAsJsonObject)
                .map(indices -> getIndexSettings(indices, indexName))
                .orElse(new JsonObject());

        return checkForReopened(indexJson);
    }

    public Map<String, Boolean> areReopened(Collection<String> indices) {
        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(new State.Builder().withMetadata().build());
        } catch (IOException e) {
            LOG.warn("Couldn't read cluster state for indices {}", indices, e);
            return Collections.emptyMap();
        }

        final JsonObject indicesJson = getClusterStateIndicesMetadata(jestResult.getJsonObject());
        return indices.stream().collect(
            Collectors.toMap(index -> index, index -> checkForReopened(getIndexSettings(indicesJson, index)))
        );
    }

    private JsonObject getIndexSettings(JsonObject indicesJson, String index) {
        return Optional.ofNullable(indicesJson)
                .filter(JsonElement::isJsonObject)
                .map(JsonElement::getAsJsonObject)
                .map(indices -> indices.get(index))
                .filter(JsonElement::isJsonObject)
                .map(JsonElement::getAsJsonObject)
                .map(idx -> idx.get("settings"))
                .filter(JsonElement::isJsonObject)
                .map(JsonElement::getAsJsonObject)
                .map(settings -> settings.get("index"))
                .filter(JsonElement::isJsonObject)
                .map(JsonElement::getAsJsonObject)
                .orElse(new JsonObject());
    }

    private boolean checkForReopened(@Nullable JsonObject indexSettings) {
        return Optional.ofNullable(indexSettings)
                .map(settings -> settings.get(REOPENED_INDEX_SETTING))
                .filter(JsonElement::isJsonPrimitive)
                .map(JsonElement::getAsJsonPrimitive)
                .map(JsonPrimitive::getAsBoolean)
                .orElse(false);
    }

    public Set<String> getClosedIndices(final IndexSet indexSet) {
        final String indexWildcard = indexSet.getIndexPrefix() + "*";
        final State request = new State.Builder().withMetadata().indices(indexWildcard).build();

        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(request);
        } catch (IOException e) {
            LOG.warn("Couldn't read cluster state for closed indices", e);
            return Collections.emptySet();
        }

        if(!jestResult.isSucceeded()) {
            LOG.warn("Couldn't read cluster state for closed indices: ", jestResult.getErrorMessage());
            return Collections.emptySet();
        }

        final JsonObject indicesJson = getClusterStateIndicesMetadata(jestResult.getJsonObject());

        final ImmutableSet.Builder<String> closedIndices = ImmutableSet.builder();
        for (Map.Entry<String, JsonElement> entry : indicesJson.entrySet()) {
            final String indexName = entry.getKey();
            final boolean isClosed = Optional.ofNullable(entry.getValue())
                    .filter(JsonElement::isJsonObject)
                    .map(JsonElement::getAsJsonObject)
                    .map(metaData -> metaData.get("state"))
                    .filter(JsonElement::isJsonPrimitive)
                    .map(JsonElement::getAsString)
                    .map("close"::equals)
                    .orElse(false);

            if (isClosed) {
                closedIndices.add(indexName);
            }
        }

        return closedIndices.build();
    }

    private JsonObject getClusterStateIndicesMetadata(JsonObject clusterStateJson) {
        // The calls to JsonElement::isJsonObject are necessary because JsonElement::getAsJsonObject
        // will throw an IllegalStateException if the JsonElement is no JsonObject.
        return Optional.ofNullable(clusterStateJson)
                .map(json -> json.get("metadata"))
                .filter(JsonElement::isJsonObject)
                .map(JsonElement::getAsJsonObject)
                .map(metadata -> metadata.get("indices"))
                .filter(JsonElement::isJsonObject)
                .map(JsonElement::getAsJsonObject)
                .orElse(new JsonObject());
    }

    public Set<String> getReopenedIndices(final IndexSet indexSet) {
        final String indexWildcard = indexSet.getIndexPrefix() + "*";
        final State request = new State.Builder().withMetadata().indices(indexWildcard).build();

        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(request);
        } catch (IOException e) {
            LOG.warn("Couldn't read cluster state for reopened indices", e);
            return Collections.emptySet();
        }

        if(!jestResult.isSucceeded()) {
            LOG.warn("Couldn't read cluster state for reopened indices: ", jestResult.getErrorMessage());
            return Collections.emptySet();
        }

        final JsonObject indicesJson = getClusterStateIndicesMetadata(jestResult.getJsonObject());
        final ImmutableSet.Builder<String> reopenedIndices = ImmutableSet.builder();

        for (Map.Entry<String, JsonElement> entry : indicesJson.entrySet()) {
            final String indexName = entry.getKey();
            final JsonElement value = entry.getValue();
            if(value.isJsonObject()) {
                final JsonObject indexSettingsJson = value.getAsJsonObject();
                final JsonObject indexSettings = getIndexSettings(indexSettingsJson, indexName);
                if(checkForReopened(indexSettings)) {
                    reopenedIndices.add(indexName);
                }
            }
        }

        return reopenedIndices.build();
    }

    @Nullable
    public IndexStatistics getIndexStats(String index) {
        final IndexStats indexStats;
        try {
            indexStats = indexStats(index);
        } catch (ElasticsearchException e) {
            return null;
        }

        if (indexStats == null) {
            return null;
        }

        final ImmutableList.Builder<ShardRouting> shardRouting = ImmutableList.builder();
        for (ShardStats shardStats : indexStats.getShards()) {
            shardRouting.add(shardStats.getShardRouting());
        }

        return IndexStatistics.create(indexStats.getIndex(), indexStats.getPrimaries(), indexStats.getTotal(), shardRouting.build());
    }

    public Set<IndexStatistics> getIndicesStats(final IndexSet indexSet) {
        final Map<String, JsonObject> responseIndices = getAll(indexSet);

        final ImmutableSet.Builder<IndexStatistics> result = ImmutableSet.builder();
        for (JsonObject indexStats : responseIndices.values()) {
            final ImmutableList.Builder<ShardRouting> shardRouting = ImmutableList.builder();
            /*
            for (ShardStats shardStats : indexStats.getShards()) {
                shardRouting.add(shardStats.getShardRouting());
            }

            final IndexStatistics stats = IndexStatistics.create(
                    indexStats.getIndex(),
                    indexStats.getPrimaries(),
                    indexStats.getTotal(),
                    shardRouting.build());

            result.add(stats);
            */
        }

        return result.build();
    }

    public boolean cycleAlias(String aliasName, String targetIndex) {
        final AddAliasMapping addAliasMapping = new AddAliasMapping.Builder(targetIndex, aliasName).build();
        try {
            return jestClient.execute(new ModifyAliases.Builder(addAliasMapping).build()).isSucceeded();
        } catch (IOException e) {
            LOG.warn("Couldn't point alias {} to index {}", aliasName, targetIndex, e);
            return false;
        }
    }

    public boolean cycleAlias(String aliasName, String targetIndex, String oldIndex) {
        final AliasMapping addAliasMapping = new AddAliasMapping.Builder(targetIndex, aliasName).build();
        final AliasMapping removeAliasMapping = new RemoveAliasMapping.Builder(oldIndex, aliasName).build();
        final ModifyAliases request = new ModifyAliases.Builder(Arrays.asList(removeAliasMapping, addAliasMapping)).build();
        try {
            return jestClient.execute(request).isSucceeded();
        } catch (IOException e) {
            LOG.warn("Couldn't switch alias {} from index {} to index {}", aliasName, oldIndex, targetIndex, e);
            return false;
        }
    }

    public boolean removeAliases(String alias, Set<String> indices) {
        final AliasMapping removeAliasMapping = new RemoveAliasMapping.Builder(ImmutableList.copyOf(indices), alias).build();
        final ModifyAliases request = new ModifyAliases.Builder(removeAliasMapping).build();
        try {
            return jestClient.execute(request).isSucceeded();
        } catch (IOException e) {
            LOG.warn("Couldn't remove alias {} from indices {}", alias, indices, e);
            return false;
        }
    }

    public void optimizeIndex(String index, int maxNumSegments, Duration timeout) {
        // TODO: Individual timeout?
        final ForceMerge request = new ForceMerge.Builder()
                .addIndex(index)
                .maxNumSegments(maxNumSegments)
                .flush(true)
                .onlyExpungeDeletes(false)
                .build();

        try {
            jestClient.execute(request);
        } catch (IOException e) {
            LOG.warn("Couldn't force merge index {}", index, e);
        }
    }

    public Health.Status waitForRecovery(String index) {
        return waitForStatus(index, Health.Status.YELLOW);
    }

    private Health.Status waitForStatus(String index, Health.Status clusterHealthStatus) {
        LOG.debug("Waiting until index health status of index {} is {}", index, clusterHealthStatus);
        final Health request = new Health.Builder()
                .addIndex(index)
                .waitForStatus(clusterHealthStatus)
                .build();
        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(request);
        } catch (IOException e) {
            LOG.warn("Couldn't read health status for index {}", index, e);
            return Health.Status.RED;
        }

        if (jestResult.isSucceeded()) {
            final String status = jestResult.getJsonObject().get("status").getAsString();
            return Health.Status.valueOf(status.toUpperCase(Locale.ENGLISH));
        } else {
            LOG.warn("Couldn't read health status for index {}: {}", index, jestResult.getErrorMessage());
            return Health.Status.RED;
        }
    }

    @Nullable
    public DateTime indexCreationDate(String index) {
        final JestResult jestResult;
        try {
            jestResult = jestClient.execute(new GetSettings.Builder().addIndex(index).build());
        } catch (IOException e) {
            LOG.warn("Couldn't settings of index {}", index, e);
            return null;
        }

        if (jestResult.isSucceeded()) {
            final JsonObject responseJson = jestResult.getJsonObject();
            final JsonObject settingsJson = responseJson.getAsJsonObject("settings");
            final JsonObject indexJson = settingsJson.getAsJsonObject("index");
            final JsonElement creationDateJson = indexJson.get("creation_date");
            if (creationDateJson == null || !creationDateJson.isJsonPrimitive()) {
                return null;
            } else {
                final long creationDate = creationDateJson.getAsLong();
                return new DateTime(creationDate, DateTimeZone.UTC);
            }
        } else {
            LOG.warn("Couldn't read settings of index {}: {}", index, jestResult.getErrorMessage());
            return null;
        }
    }

    /**
     * Calculate min and max message timestamps in the given index.
     *
     * @param index Name of the index to query.
     * @return the timestamp stats in the given index, or {@code null} if they couldn't be calculated.
     * @see org.elasticsearch.search.aggregations.metrics.stats.Stats
     */
    public IndexRangeStats indexRangeStatsOfIndex(String index) {
        final FilterAggregationBuilder builder = AggregationBuilders.filter("agg")
                .filter(QueryBuilders.existsQuery("timestamp"))
                .subAggregation(AggregationBuilders.min("ts_min").field("timestamp"))
                .subAggregation(AggregationBuilders.max("ts_max").field("timestamp"))
                .subAggregation(AggregationBuilders.terms("streams").field("streams"));
        final SearchRequestBuilder srb = c.prepareSearch()
                .setIndices(index)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setSize(0)
                .addAggregation(builder);

        final SearchResponse response;
        try {
            final SearchRequest request = srb.request();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Index range query: _search/{}: {}",
                        index,
                        XContentHelper.convertToJson(request.source(), false));
            }
            response = c.search(request).actionGet();
        } catch (IndexClosedException e) {
            throw e;
        } catch (org.elasticsearch.index.IndexNotFoundException e) {
            LOG.error("Error while calculating timestamp stats in index <" + index + ">", e);
            throw e;
        } catch (ElasticsearchException e) {
            LOG.error("Error while calculating timestamp stats in index <" + index + ">", e);
            throw new org.elasticsearch.index.IndexNotFoundException("Index " + index + " not found", e);
        } catch (IOException e) {
            // can potentially happen when recreating the source of
            // the index range aggregation query on DEBUG (via XContentHelper)
            throw new RuntimeException(e);
        }

        final Filter f = response.getAggregations().get("agg");
        if (f.getDocCount() == 0L) {
            LOG.debug("No documents with attribute \"timestamp\" found in index <{}>", index);
            return IndexRangeStats.EMPTY;
        }

        final Min minAgg = f.getAggregations().get("ts_min");
        final DateTime min = new DateTime((long) minAgg.getValue(), DateTimeZone.UTC);
        final Max maxAgg = f.getAggregations().get("ts_max");
        final DateTime max = new DateTime((long) maxAgg.getValue(), DateTimeZone.UTC);
        // make sure we return an empty list, so we can differentiate between old indices that don't have this information
        // and newer ones that simply have no streams.
        ImmutableList.Builder<String> streamIds = ImmutableList.builder();
        final Terms streams = f.getAggregations().get("streams");
        if (!streams.getBuckets().isEmpty()) {
            streamIds.addAll(streams.getBuckets().stream()
                    .map(Terms.Bucket::getKeyAsString)
                    .collect(toSet()));
        }

        return IndexRangeStats.create(min, max, streamIds.build());
    }
}
