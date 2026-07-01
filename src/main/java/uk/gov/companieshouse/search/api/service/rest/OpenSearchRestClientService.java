package uk.gov.companieshouse.search.api.service.rest;


import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateRequest;
import org.opensearch.client.opensearch.core.UpdateResponse;


import java.io.IOException;

public interface OpenSearchRestClientService {

    /**
     * interface for Open search high level rest client used for search
     *
     * @param searchRequest - searchRequest containing search parameters
     * @return SearchResponse - response from Open search db
     */
    SearchResponse<Object> search(SearchRequest searchRequest) throws IOException;

    /**
     * interface for elastic search high level rest client used in upsert
     *
     * @param updateRequest - updateRequest containing update parameters
     * @return UpdateResponse - response from elastic search db
     */
    UpdateResponse<Object> upsert(UpdateRequest updateRequest) throws IOException;

}
