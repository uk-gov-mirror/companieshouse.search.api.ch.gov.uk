package uk.gov.companieshouse.search.api.service.search.impl.alphabetical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import uk.gov.companieshouse.search.api.elasticsearch.AlphabeticalSearchRequests;
import uk.gov.companieshouse.search.api.exception.SearchException;
import uk.gov.companieshouse.search.api.logging.LoggingUtils;
import uk.gov.companieshouse.search.api.model.SearchResults;
import uk.gov.companieshouse.search.api.model.esdatamodel.company.Company;
import uk.gov.companieshouse.search.api.model.esdatamodel.company.Items;
import uk.gov.companieshouse.search.api.model.esdatamodel.company.Links;
import uk.gov.companieshouse.search.api.model.response.AlphaKeyResponse;
import uk.gov.companieshouse.search.api.service.AlphaKeyService;
import uk.gov.companieshouse.search.api.service.search.SearchRequestService;

@Service
public class AlphabeticalSearchRequestService implements SearchRequestService {

    @Autowired
    private AlphaKeyService alphaKeyService;
    @Autowired
    private AlphabeticalSearchRequests alphabeticalSearchRequests;

    private static final String ORDERED_ALPHA_KEY_WITH_ID = "ordered_alpha_key_with_id";
    private static final int FALLBACK_QUERY_LIMIT = 25;

    /**
     * {@inheritDoc}
     */
    @Override
    public SearchResults getAlphabeticalSearchResults(String corporateName, String requestId) throws SearchException {
        Map<String, Object> logMap = LoggingUtils.createLoggingMap(requestId);
        logMap.put(LoggingUtils.COMPANY_NAME, corporateName);
        logMap.put(LoggingUtils.INDEX, LoggingUtils.INDEX_ALPHABETICAL);

        LoggingUtils.getLogger().info("Performing search request", logMap);

        String orderedAlphakey = "";
        String topHitCompanyName = "";
        List<Company> results = new ArrayList<>();

        AlphaKeyResponse alphaKeyResponse = alphaKeyService.getAlphaKeyForCorporateName(corporateName);
        if (alphaKeyResponse != null) {
            orderedAlphakey = alphaKeyResponse.getOrderedAlphaKey();
            logMap.put(LoggingUtils.ORDERED_ALPHAKEY, orderedAlphakey);
        }

        try {
            SearchHits hits =  getSearchHits(orderedAlphakey, requestId);

            if (hits.getTotalHits().value == 0) {
                LoggingUtils.getLogger().info("A result was not found, reducing search term to find result", logMap);

                hits = peelbackSearchRequest(hits, orderedAlphakey, requestId);
            }

            if (hits.getTotalHits().value > 0) {
                LoggingUtils.getLogger().info("A result has been found", logMap);

                String orderedAlphakeyWithId;
                SearchHit topHit;
                orderedAlphakeyWithId = getOrderedAlphaKeyWithId(hits.getHits()[0]);
                topHit = hits.getHits()[0];

                Company topHitCompany = getCompany(topHit);
                topHitCompanyName = topHitCompany.getItems().getCorporateName();

                populateSearchResults(requestId, topHitCompanyName, results, topHitCompany, orderedAlphakeyWithId);
            }
        } catch (IOException e) {
            LoggingUtils.getLogger().error("failed to map highest map to company object", logMap);
            throw new SearchException("error occurred reading data for highest match from " +
                "searchHits", e);
        }
        return new SearchResults("", topHitCompanyName, results);
    }


    public SearchHits peelbackSearchRequest(SearchHits hits, String orderedAlphakey,
                                            String requestId) throws IOException {
        for (int i = 0; i < orderedAlphakey.length(); i++) {

            if (hits.getTotalHits().value > 0 || i == FALLBACK_QUERY_LIMIT) {
                return hits;
            }

            if (i != orderedAlphakey.length() - 1) {
                String resultString = orderedAlphakey.substring(0, orderedAlphakey.length() - i);
                hits = getSearchHits(resultString, requestId);
            }
        }
        return hits;
    }

    private SearchHits getSearchHits(String orderedAlphakey, String requestId) throws IOException {
        SearchHits hits =  alphabeticalSearchRequests
                .getBestMatchResponse(orderedAlphakey, requestId);

        if (hits.getTotalHits().value == 0) {
            hits = alphabeticalSearchRequests
                    .getStartsWithResponse(orderedAlphakey, requestId);
        }

        if (hits.getTotalHits().value == 0) {
            hits = alphabeticalSearchRequests
                    .getCorporateNameStartsWithResponse(orderedAlphakey, requestId);
        }
        return hits;
    }

    private void populateSearchResults(String requestId,
                                       String topHitCompanyName,
                                       List<Company> results,
                                       Company topHitCompany,
                                       String orderedAlphakeyWithId) throws IOException {
        SearchHits hits;
        hits = alphabeticalSearchRequests.getAboveResultsResponse(requestId, orderedAlphakeyWithId,
            topHitCompanyName);
        hits.forEach(h -> results.add(getCompany(h)));

        Collections.reverse(results);

        LoggingUtils.getLogger().info("Retrieving the top hit: " + topHitCompanyName);
        results.add(topHitCompany);

        hits = alphabeticalSearchRequests.getDescendingResultsResponse(requestId, orderedAlphakeyWithId,
            topHitCompanyName);

        hits.forEach(h -> results.add(getCompany(h)));
    }

    private String getOrderedAlphaKeyWithId(SearchHit hit) {
        Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        return (String) sourceAsMap.get(ORDERED_ALPHA_KEY_WITH_ID);
    }

    private Company getCompany(SearchHit hit) {
        Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        Map<String, Object> items = (Map<String, Object>) sourceAsMap.get("items");
        Map<String, Object> links = (Map<String, Object>) sourceAsMap.get("links");

        Company company = new Company();
        Items companyItems = new Items();
        Links companyLinks = new Links();

        companyItems.setCorporateName((String) (items.get("corporate_name")));
        companyItems.setCompanyNumber((String) (items.get("company_number")));
        companyItems.setCompanyStatus((String) (items.get("company_status")));

        companyLinks.setSelf((String) (links.get("self")));

        company.setId((String) sourceAsMap.get("ID"));
        company.setCompanyType((String) sourceAsMap.get("company_type"));
        company.setItems(companyItems);
        company.setLinks(companyLinks);

        return company;
    }
}
