package uk.gov.companieshouse.search.api.service.search.impl.alphabetical;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.search.api.exception.ObjectMapperException;
import uk.gov.companieshouse.search.api.exception.SearchException;
import uk.gov.companieshouse.search.api.model.SearchResults;
import uk.gov.companieshouse.search.api.model.esdatamodel.company.Company;
import uk.gov.companieshouse.search.api.model.esdatamodel.company.Items;
import uk.gov.companieshouse.search.api.model.response.ResponseObject;
import uk.gov.companieshouse.search.api.model.response.ResponseStatus;
import uk.gov.companieshouse.search.api.service.search.SearchIndexService;
import uk.gov.companieshouse.search.api.service.search.SearchRequestService;
import uk.gov.companieshouse.search.api.service.rest.RestClientService;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static uk.gov.companieshouse.search.api.SearchApiApplication.APPLICATION_NAME_SPACE;

@Service
public class AlphabeticalSearchIndexService implements SearchIndexService {

    @Autowired
    private RestClientService searchRestClient;

    @Autowired
    private SearchRequestService searchRequestService;

    private static final String HIGHEST_MATCH = "highest_match";

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAME_SPACE);

    private static final String ALPHABETICAL_SEARCH = "Alphabetical Search: ";

    private static final String SEARCH_TYPE = "alphabetical_search";
    private static final String SPECIAL_CHARACTERS = "+*^.@&!?$£";
    private static final String SPACE_CHARACTER = " ";
    // spaces around company endings help in finding exact matches
    private static final String COMPANY_NAME_ENDINGS = " AEIE | ANGHYFYNGEDIG | C.B.C | C.B.C. | C.C.C | C.C.C. " +
            "| C.I.C. | CBC | CBCN | CBP | CCC | CCG CYF | CCG CYFYNGEDIG | CIC | COMMUNITY INTEREST COMPANY " +
            "| COMMUNITY INTEREST P.L.C. | COMMUNITY INTEREST PLC | COMMUNITY INTEREST PUBLIC LIMITED COMPANY " +
            "| CWMNI BUDDIANT CYMUNEDOL | CWMNI BUDDIANT CYMUNEDOL C.C.C | CWMNI BUDDIANT CYMUNEDOL CCC " +
            "| CWMNI BUDDIANT CYMUNEDOL CYHOEDDUS CYFYNGEDIG | CWMNI BUDDSODDIA CHYFALAF NEWIDIOL " +
            "| CWMNI BUDDSODDIANT PENAGORED | CWMNI CELL GWARCHODEDIG | CWMNI CYFYNGEDIG CYHOEDDUS | CYF | CYF. " +
            "| CYFYNGEDIG | EEIG | EESV | EOFG | EOOS | EUROPEAN ECONOMIC INTEREST GROUPING | GEIE | GELE | ICVC " +
            "| INVESTMENT COMPANY WITH VARIABLE CAPITAL | L.P. | LIMITED | LIMITED LIABILITY PARTNERSHIP " +
            "| LIMITED PARTNERSHIP | LLP | LP | LTD | LTD. | OEIC | OPEN-ENDED INVESTMENT COMPANY | P.C. | P.L.C " +
            "| P.L.C. | PAC | PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG | PARTNERIAETH CYFYNGEDIG | PC | PCC LIMITED " +
            "| PCC LTD | PLC | PROTECTED CELL COMPANY | PUBLIC LIMITED COMPANY | SE | UNLIMITED | UNLTD | UNLTD. ";

    /**
     * {@inheritDoc}
     */
    @Override
    public ResponseObject search(String corporateName, String requestId) {

        SearchResults searchResults;

        try {
            LOG.info(ALPHABETICAL_SEARCH + "started for: " + corporateName);
            searchResults = performAlphabeticalSearch(corporateName, requestId);
        } catch (SearchException | ObjectMapperException e) {
            LOG.error("An error occurred in alphabetical search whilst searching: " + corporateName, e);
            return new ResponseObject(ResponseStatus.SEARCH_ERROR, null);
        }

        if(searchResults.getResults() != null) {
            LOG.info(ALPHABETICAL_SEARCH + "successful for: " + corporateName);
            return new ResponseObject(ResponseStatus.SEARCH_FOUND, searchResults);
        }

        LOG.info(ALPHABETICAL_SEARCH + "No results were returned while searching: " + corporateName);
        return new ResponseObject(ResponseStatus.SEARCH_NOT_FOUND, null);
    }

    private SearchResults performAlphabeticalSearch(String corporateName, String requestId)
        throws SearchException, ObjectMapperException {

        SearchResponse searchResponse;

        try {
            searchResponse = searchRestClient.searchRestClient(
                searchRequestService.createSearchRequest(corporateName, requestId));
        } catch (IOException e) {
            LOG.error(ALPHABETICAL_SEARCH + "Failed to get a search response from elastic search " +
                "for: " + corporateName, e);
            throw new SearchException("Error occurred while searching index", e);
        }

        String highestMatchName = null;
        if(searchResponse != null && searchResponse.getAggregations() != null) {
            highestMatchName = getAggregatedSearchResults(
                searchResponse.getAggregations().asList(), corporateName);
        }

        LOG.info("Found hits: " + searchResponse.getHits().getHits().length);
        if(highestMatchName != null) {
            return getSearchResults(highestMatchName, searchResponse.getHits(), corporateName);
        } else {
            LOG.info(ALPHABETICAL_SEARCH + "Could not locate a highest match in the search " +
                "aggregation for: " + corporateName);
            throw new SearchException("highest match was not located in the search, unable to " +
                "process search request");
        }
    }

    private String getAggregatedSearchResults(List<Aggregation> aggregations, String corporateName)
        throws ObjectMapperException {

        // loop the aggregations to obtain the highest match.
        for (Aggregation aggregation : aggregations) {

            if (aggregation.getName().equals(HIGHEST_MATCH)) {
                return getHighestMatchedCompanyName(aggregation, corporateName);
            }
        }
        return null;
    }

    private SearchResults<Company> getSearchResults(String highestMatchName, SearchHits searchHits,
        String corporateName)
        throws ObjectMapperException {

        SearchResults<Company> searchResults = new SearchResults();

        String bestMatchName = new String();
        int bestMatchIndexPos = 0;
        List<Company> companies = getCompaniesFromSearchHits(searchHits, corporateName);

        /// find the company that matches exactly with the requested name
        for(Company company : companies) {
            String strippedCorpName = stripCorporateEnding(corporateName);
            // if this is a search for a special character (single letter)
            if (SPECIAL_CHARACTERS.contains(strippedCorpName) || strippedCorpName.matches("([\\s+*^.$?@&!£]+)")){
                bestMatchName = company.getItems().getCorporateName();
                break;
            }
            // find and return the first match amongst the es results
            if (company.getItems().getCorporateName()
                    .matches(strippedCorpName.toUpperCase() + "([\\s?0-9A-Z])?.*")) {
                bestMatchName = company.getItems().getCorporateName();
                break;
            }
            bestMatchIndexPos++;
        }

        searchResults = getBestMatchSearchResults(companies, bestMatchIndexPos, bestMatchName);

        /// return results found around best match OR fall back to elasticsearch's highest match
        if (searchResults != null && searchResults.getResults().size() > 0) {
            return searchResults;
        }

        int highestMatchIndexPos = 0;
        // find the pos in the list of companies where the highest match is.
        for(Company company : companies) {
            if (company.getItems().getCorporateName().equals(highestMatchName)) {
                searchResults = getAlphabeticalSearchResults(companies,
                    highestMatchIndexPos, highestMatchName);
            }
            highestMatchIndexPos++;
        }

        return searchResults;
    }

    private SearchResults<Company> getBestMatchSearchResults(List<Company> companies,
                                                             int bestMatchIndexPos, String bestMatchName){
        List<Company> searchCompanyResults = new ArrayList<>();

        int totalResults = companies.size();

        int startIndex = getIndexStart(bestMatchIndexPos);
        int endIndex = getIndexEnd(totalResults, bestMatchIndexPos);

        // loop to get 20 hits with 9 records above and 10 below the highest match.
        for(int i = startIndex; i < endIndex; i++) {
            searchCompanyResults.add(companies.get(i));
        }

        return new SearchResults<>(SEARCH_TYPE, bestMatchName, searchCompanyResults);
    }

    private SearchResults<Company> getAlphabeticalSearchResults(List<Company> companies,
        int highestMatchIndexPos, String highestMatchName) {

        List<Company> searchCompanyResults = new ArrayList<>();

        int totalResults = companies.size();

        int startIndex = getIndexStart(highestMatchIndexPos);
        int endIndex = getIndexEnd(totalResults, highestMatchIndexPos);

        // loop to get 20 hits with 9 records above and 10 below the highest match.
        for(int i = startIndex; i < endIndex; i++) {
            searchCompanyResults.add(companies.get(i));
        }

        return new SearchResults<>(SEARCH_TYPE, highestMatchName, searchCompanyResults);
    }

    private int getIndexEnd(int totalResults, int highestMatchIndexPos) {

        int bottomMatchesSize = totalResults - highestMatchIndexPos;
        int indexEndPos = highestMatchIndexPos + (bottomMatchesSize < 10 ? bottomMatchesSize : 10);
        int differenceIndexPos = indexEndPos - totalResults;

        if (differenceIndexPos <= 0) {
            return indexEndPos;
        } else {
            return indexEndPos - differenceIndexPos;
        }
    }

    private int getIndexStart(int highestMatchIndexPos) {

        int indexStartPos = highestMatchIndexPos - 9;

        if (indexStartPos >= 0) {
            return indexStartPos;
        } else {
            return 0;
        }
    }

    private String getHighestMatchedCompanyName(Aggregation aggregation,
        String corporateName) throws ObjectMapperException {

        SearchHits searchHitsHighestMatched = transformToSearchHits(aggregation);

        if (searchHitsHighestMatched.getHits().length == 0){
            return null;
        }

        Optional<Company> companyTopHit;

        try {
            // extract the highest matched name from position 0 as we know there is only one.
            companyTopHit = Optional.of(new ObjectMapper()
                .readValue(searchHitsHighestMatched
                    .getAt(0)
                    .getSourceAsString(), Company.class));

        } catch (IOException e) {
            LOG.error(ALPHABETICAL_SEARCH + "failed to map highest map to company object for: " + corporateName, e);
            throw new ObjectMapperException("error occurred reading data for highest match from " +
                "searchHits", e);
        }

        // return the name of highest match.
        return companyTopHit.map(Company::getItems)
            .map(Items::getCorporateName)
            .orElse("");
    }

    private SearchHits transformToSearchHits(Aggregation aggregation) {

        TopHits topHits = (TopHits) aggregation;
        return topHits.getHits();
    }

    private String stripCorporateEnding(String corporateName){
        if (corporateName.contains(SPACE_CHARACTER)){
            int corpNameEndingStart = corporateName.lastIndexOf(SPACE_CHARACTER);
            String corpNameEnding = corporateName.substring(corpNameEndingStart).trim();
            if (COMPANY_NAME_ENDINGS.contains(" " + corpNameEnding.toUpperCase() + " ")) {
                String corpNameSansEnding = corporateName.trim().substring(0, corporateName.lastIndexOf(SPACE_CHARACTER));
                return corpNameSansEnding;
            }
        }
        return corporateName;
    }

    // comparator to order strings ignoring whitespaces and special characters
    private Comparator<Company> companyNameNoSpacesComparator(){
        String pattern = "[^A-Za-z]+";
        String replacement = "";
        return Comparator.comparing(c -> stripCorporateEnding(c.getItems().getCorporateName()).replaceAll(pattern, replacement));
    }

    // comparator to order strings ignoring whitespaces
    private Comparator<Company> companyNameSpecialCharsComparator(){
        String pattern = "[^A-Za-z+$?.*^@&!£]+";
        String replacement = "";
        return Comparator.comparing(c -> stripCorporateEnding(c.getItems().getCorporateName()).replaceAll(pattern, replacement));
    }

    private List<Company> getCompaniesFromSearchHits(SearchHits searchHits,
        String corporateName) throws ObjectMapperException {

        List<Company> companies = new ArrayList<>();

        // loop and map companies from search hits to Company model
        for(SearchHit searchHit : searchHits.getHits()) {

            Company company;

            try {
                company = new ObjectMapper().readValue(searchHit.getSourceAsString(), Company.class);
            } catch (IOException e) {
                LOG.error(ALPHABETICAL_SEARCH + "failed to map search hit to company object for: " + corporateName, e);
                throw new ObjectMapperException("error occurred reading data for company from " +
                    "searchHits", e);
            }

            companies.add(company);
        }

        String corpNameFirstLetter = corporateName.toUpperCase().substring(0,1);

        // if searching for single character
        if (stripCorporateEnding(corporateName).length() == 1 && SPECIAL_CHARACTERS.contains(corpNameFirstLetter)) {
            companies.stream()
                    .sorted(companyNameSpecialCharsComparator())
                    .forEach(c -> {System.out.println(c.getItems().getCorporateName());});

            return companies.stream()
                    .sorted(companyNameSpecialCharsComparator())
                    .collect(Collectors.toList());
        }
        System.out.println("##############  " + corporateName + "  ##############");
        companies.stream()
                .filter(c -> c.getItems().getCorporateName().startsWith(corpNameFirstLetter))
                .sorted(companyNameNoSpacesComparator())
                .forEach(c -> {System.out.println(c.getItems().getCorporateName());});


        return companies.stream()
                .filter(c -> c.getItems().getCorporateName().startsWith(corpNameFirstLetter))
                .sorted(companyNameNoSpacesComparator())
            .collect(Collectors.toList());
    }
}

