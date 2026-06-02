package uk.gov.companieshouse.search.api.service.rest;


import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import java.io.IOException;

public interface OpenSearchRestClientService {

    /**
     * interface for elastic search high level rest client used for search
     *
     * @param searchRequest - searchRequest containing search parameters
     * @return SearchResponse - response from elastic search db
     */
    SearchResponse<Object> search(SearchRequest searchRequest) throws IOException;

}
