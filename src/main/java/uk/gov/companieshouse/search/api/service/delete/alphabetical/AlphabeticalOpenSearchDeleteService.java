package uk.gov.companieshouse.search.api.service.delete.alphabetical;

import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.search.api.logging.LoggingUtils;
import uk.gov.companieshouse.search.api.model.response.ResponseObject;
import uk.gov.companieshouse.search.api.model.response.ResponseStatus;
import uk.gov.companieshouse.search.api.service.rest.impl.AlphabeticalOpenSearchRestClientService;
import uk.gov.companieshouse.search.api.util.ConfiguredIndexNamesProvider;
import org.opensearch.client.opensearch._types.Result;

import java.io.IOException;
import java.util.Map;

import static uk.gov.companieshouse.search.api.logging.LoggingUtils.getLogger;

@Service
@ConditionalOnProperty(name = "search.backend", havingValue = "opensearch")
public class AlphabeticalOpenSearchDeleteService implements AlphabeticalSearchDeleteService {

    private final AlphabeticalOpenSearchRestClientService alphabeticalSearchRestClientService;

    private final ConfiguredIndexNamesProvider indices;

    public AlphabeticalOpenSearchDeleteService(AlphabeticalOpenSearchRestClientService alphabeticalSearchRestClientService,
                                               ConfiguredIndexNamesProvider indices) {
        this.alphabeticalSearchRestClientService = alphabeticalSearchRestClientService;
        this.indices = indices;
    }

    public ResponseObject<String> deleteCompany(String companyNumber) {

        Map<String, Object> logMap =
                LoggingUtils.setUpAlphabeticalSearchDeleteLogging(companyNumber, indices);
        getLogger().info("Deleting company on OpenSearch underway", logMap);

        DeleteRequest deleteRequest = new DeleteRequest.Builder()
                .index(indices.alphabetical())
                .id(companyNumber)
                .build();

        DeleteResponse response;
        try {
            response = alphabeticalSearchRestClientService.delete(deleteRequest);
        } catch (IOException e) {
            getLogger().error(String.format("IOException encountered when deleting [%s] from the alphabetical OpenSearch index",
                    companyNumber), logMap);
            return new ResponseObject<>(ResponseStatus.SERVICE_UNAVAILABLE);
        } catch (OpenSearchException e) {
            return new ResponseObject<>(ResponseStatus.DELETE_REQUEST_ERROR);
        }

        if (response.result() == Result.NotFound) {
            getLogger().error(String.format("Document with id: [%s] not found in alphabetical OpenSearch index",
                    companyNumber), logMap);
            return new ResponseObject<>(ResponseStatus.DELETE_NOT_FOUND);
        } else {
            getLogger().info(String.format("Successfully deleted [%s] from alphabetical OpenSearch index",
                    companyNumber), logMap);
            return new ResponseObject<>(ResponseStatus.DOCUMENT_DELETED);
        }
    }


}
