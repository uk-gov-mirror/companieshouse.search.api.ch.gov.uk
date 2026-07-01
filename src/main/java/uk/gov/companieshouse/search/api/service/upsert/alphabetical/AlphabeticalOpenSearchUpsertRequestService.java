package uk.gov.companieshouse.search.api.service.upsert.alphabetical;


import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.UpdateRequest;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.api.model.company.CompanyProfileApi;
import uk.gov.companieshouse.logging.util.DataMap;
import uk.gov.companieshouse.search.api.exception.UpsertException;
import uk.gov.companieshouse.search.api.logging.LoggingUtils;
import uk.gov.companieshouse.search.api.model.response.AlphaKeyResponse;
import uk.gov.companieshouse.search.api.opensearch.AlphabeticalOpenSearchUpsertRequest;
import uk.gov.companieshouse.search.api.service.AlphaKeyService;
import uk.gov.companieshouse.search.api.util.ConfiguredIndexNamesProvider;

import java.util.Map;

@Service
public class AlphabeticalOpenSearchUpsertRequestService {

    private final AlphaKeyService alphaKeyService;

    private final AlphabeticalOpenSearchUpsertRequest alphabeticalSearchUpsertRequest;

    private final ConfiguredIndexNamesProvider indices;

    public AlphabeticalOpenSearchUpsertRequestService(AlphaKeyService alphaKeyService,
                                                      AlphabeticalOpenSearchUpsertRequest alphabeticalSearchUpsertRequest,
                                                      ConfiguredIndexNamesProvider indices) {
        this.alphaKeyService = alphaKeyService;
        this.alphabeticalSearchUpsertRequest = alphabeticalSearchUpsertRequest;
        this.indices = indices;
    }

    /**
     * Create an index request for document if it does not currently exist
     * @param company - Company sent over in REST call to be added/updated
     * @return {@link IndexRequest}
     * @throws UpsertException
     */
    public IndexRequest createIndexRequest(CompanyProfileApi company) throws UpsertException {

        Map<String, Object> logMap = new DataMap.Builder()
                .companyName(company.getCompanyName())
                .companyNumber(company.getCompanyNumber())
                .indexName(indices.alphabetical())
                .build().getLogMap();

        String orderedAlphaKey = "";
        String orderedAlphaKeyWithID = "";

        AlphaKeyResponse alphaKeyResponse = alphaKeyService.getAlphaKeyForCorporateName(company.getCompanyName());
        if (alphaKeyResponse != null) {
            orderedAlphaKey = alphaKeyResponse.getOrderedAlphaKey();
            orderedAlphaKeyWithID = alphaKeyResponse.getOrderedAlphaKey() + ":" + company.getCompanyNumber();
            logMap.put(LoggingUtils.ORDERED_ALPHAKEY, orderedAlphaKey);
        }

        LoggingUtils.getLogger().info("Preparing index request", logMap);


        Map<String, Object> source = alphabeticalSearchUpsertRequest.buildRequest(company, orderedAlphaKey, orderedAlphaKeyWithID);

        return IndexRequest.of(i -> i
                .index(indices.alphabetical())
                .id(company.getCompanyNumber())
                .document(source)
        );
    }

    /**
     * If document already exists attempt to upsert the document
     * @param company - Company sent over in REST call to be added/updated
     * @param indexRequest
     * @return {@link UpdateRequest}
     * @throws UpsertException
     */
    public UpdateRequest createUpdateRequest(CompanyProfileApi company, IndexRequest indexRequest)
        throws UpsertException {
        Map<String, Object> logMap = new DataMap.Builder()
                .companyName(company.getCompanyName())
                .companyNumber(company.getCompanyNumber())
                .indexName(indices.alphabetical())
                .build().getLogMap();

        String orderedAlphaKey = "";
        String orderedAlphaKeyWithID = "";

        AlphaKeyResponse alphaKeyResponse = alphaKeyService.getAlphaKeyForCorporateName(company.getCompanyName());
        if (alphaKeyResponse != null) {
            orderedAlphaKey = alphaKeyResponse.getOrderedAlphaKey();
            logMap.put(LoggingUtils.ORDERED_ALPHAKEY, orderedAlphaKey);
            orderedAlphaKeyWithID = alphaKeyResponse.getOrderedAlphaKey() + ":" + company.getCompanyNumber();
        }

        LoggingUtils.getLogger().info("Attempt to upsert document if it does not exist", logMap);


        Map<String, Object> doc = alphabeticalSearchUpsertRequest.buildRequest(company, orderedAlphaKey, orderedAlphaKeyWithID);

        return UpdateRequest.of(u -> u
                .index(indices.alphabetical())
                .id(company.getCompanyNumber())
                .doc(doc)
                .docAsUpsert(true)
                .upsert(indexRequest)
        );

    }
}
