package uk.gov.companieshouse.search.api.service.rest.impl;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.search.api.service.rest.OpenSearchRestClientService;

import java.io.IOException;

import static org.elasticsearch.client.RequestOptions.DEFAULT;

@Service
public class AlphabeticalOpenSearchRestClientService implements OpenSearchRestClientService {

    private final OpenSearchClient alphabeticalOpenSearchClient;

    public AlphabeticalOpenSearchRestClientService(
            OpenSearchClient alphabeticalOpenSearchClient
    ) {
        this.alphabeticalOpenSearchClient = alphabeticalOpenSearchClient;
    }

    @Override
    public SearchResponse<Object> search(SearchRequest searchRequest) throws IOException {
        return alphabeticalOpenSearchClient.search(searchRequest, Object.class);
    }

}
