package uk.gov.companieshouse.search.api.service.search.impl.alphabetical;

import com.sun.org.apache.bcel.internal.generic.MULTIANEWARRAY;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import uk.gov.companieshouse.environment.EnvironmentReader;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.search.api.model.alphakeyService.AlphaKeyResponse;
import uk.gov.companieshouse.search.api.service.search.AlphaKeyService;
import uk.gov.companieshouse.search.api.service.search.SearchRequestService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static uk.gov.companieshouse.search.api.SearchApiApplication.APPLICATION_NAME_SPACE;

@Service
public class AlphabeticalSearchRequestService implements SearchRequestService {

    @Autowired
    private EnvironmentReader environmentReader;
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private AlphaKeyService alphaKeyService;

    private static final String INDEX = "ALPHABETICAL_SEARCH_INDEX";

    private static final String RESULTS_SIZE = "ALPHABETICAL_SEARCH_RESULT_MAX";
    private static final int AGGS_HIGHEST_MATCH_SIZE = 1;
    private static final String HIGHEST_MATCH = "highest_match";

    private static final String ALPHABETICAL_SEARCH = "Alphabetical Search: ";
    private static final String SPECIAL_CHARACTERS = "+*^.@&!?$£<->/{}";

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAME_SPACE);

    private static final String SPACE_CHARACTER = " ";
    // spaces and pipes around company endings help in finding exact matches
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
    public SearchRequest createSearchRequest(String corporateName, String requestId) {

        LOG.info(ALPHABETICAL_SEARCH + "Creating search request for: " + corporateName
                + " for user with Id: " + requestId);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(environmentReader.getMandatoryString(INDEX));
        searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        searchRequest.preference(requestId);
        searchRequest.source(createSource(corporateName));

        return searchRequest;
    }

    private SearchSourceBuilder createSource(String corporateName) {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //sourceBuilder.size(Integer.parseInt(environmentReader.getMandatoryString(RESULTS_SIZE)));
        sourceBuilder.size(1000);
        sourceBuilder.query(createAlphabeticalSearchQuery(corporateName));
        sourceBuilder.aggregation(createAggregation(HIGHEST_MATCH, AGGS_HIGHEST_MATCH_SIZE, corporateName));

        return sourceBuilder;
    }

    private AggregationBuilder createAggregation(String aggregationName, int size, String corporateName) {

        LOG.info(ALPHABETICAL_SEARCH + "Adding top hit aggregation for: " + corporateName);

        return AggregationBuilders
            .topHits(aggregationName)
            .size(size);
    }

    private String stripCorporateEnding(String corporateName){
        if (corporateName.contains(SPACE_CHARACTER)){
            int corpNameEndingStart = corporateName.lastIndexOf(SPACE_CHARACTER);
            String corpNameEnding = corporateName.substring(corpNameEndingStart).trim().toUpperCase();
            if (COMPANY_NAME_ENDINGS.contains(" " + corpNameEnding.toUpperCase() + " ")) {
                String corpNameSansEnding = corporateName.trim().substring(0, corporateName.lastIndexOf(SPACE_CHARACTER)).trim();
                return corpNameSansEnding;
            }
        }
        return corporateName;
    }

    private char previousCharacter(char c){
        if ((c > 48 && c < 57) || (c > 65 && c < 90) || (c > 97 && c < 122)) {
            return (char) (c - 1);
        }
        else if (c == 65){
            return '9';
        }
        else if (c == 97){
            return '9';
        }
        else {
            return c;
        }
    }

    private String appendPreviousCharacter(String corporateName){
        int corpNameLength = corporateName.length();
        char prevChar = previousCharacter(corporateName.charAt(corpNameLength-1));
        return corporateName.substring(0, corpNameLength-1) + prevChar + ((prevChar == 65 || prevChar == 97) ? "z" : "");
    }

    private List<String> generateCompanyNameAlphaKeyPrefixes(String companyNameAlphaKey){
        int corpNamePrefixLength = companyNameAlphaKey.length();
        List<String> prefixes = new ArrayList<>();
        String corpNamePrefix = "";

        if (corpNamePrefixLength == 1){
            prefixes.add(companyNameAlphaKey);
        }
        else if (corpNamePrefixLength > 10){
            corpNamePrefix = companyNameAlphaKey.substring(0, 10);
            corpNamePrefixLength = corpNamePrefix.length();
        }
        else {
            corpNamePrefix = companyNameAlphaKey;
            corpNamePrefixLength = corpNamePrefix.length();
        }
        if (corpNamePrefixLength >= 2){
            while (corpNamePrefixLength > 2) {
                prefixes.add(corpNamePrefix.substring(0, corpNamePrefixLength));
                corpNamePrefixLength--;
            }
        }

        return prefixes;
    }

    private List<String> generateCompanyNamePrefixes(String companyName){
        String corporateNameSansEnding = stripCorporateEnding(companyName).toLowerCase();
        int corpNameLength = corporateNameSansEnding.length();
        String corpNameFirstPart = "";
        int corpNameFirstPartLength;
        List<String> prefixes = new ArrayList<>();

        if (corpNameLength == 1){
            prefixes.add(companyName);
            if (!SPECIAL_CHARACTERS.contains(corporateNameSansEnding)){
                prefixes.add(previousCharacter(corporateNameSansEnding.charAt(0)) + "");
            }
        }
        else {
            if (!corporateNameSansEnding.contains(SPACE_CHARACTER)){ // single word company name
                corpNameFirstPart = corporateNameSansEnding;
            }
            else { // multi-word company name
                corpNameFirstPart = corporateNameSansEnding.substring(0, corporateNameSansEnding.indexOf(SPACE_CHARACTER)).trim();
            }
        }
        corpNameFirstPartLength = corpNameFirstPart.length();
        if (corpNameFirstPartLength > 2){
            while (corpNameFirstPartLength > 2) {
                prefixes.add(corpNameFirstPart.substring(0, corpNameFirstPartLength));
                corpNameFirstPartLength--;
            }
        }
        if (corporateNameSansEnding.contains(SPACE_CHARACTER)){
            corpNameFirstPart = corpNameFirstPart + corporateNameSansEnding.charAt(corpNameFirstPartLength + 1);
            corpNameFirstPartLength += 2;
            while (corpNameFirstPartLength > 2) {
                prefixes.add(corpNameFirstPart.substring(0, corpNameFirstPartLength));
                corpNameFirstPartLength--;
            }
        }
        else {
            if (corpNameLength < 5) {
                prefixes.add(corporateNameSansEnding.substring(0, corpNameLength - 1));
            }
            else {
                while (corpNameLength > 2) {
                    prefixes.add(corporateNameSansEnding.substring(0, corpNameLength));
                    corpNameLength--;
                }
            }
        }

        return prefixes;
    }

    private QueryBuilder createAlphabeticalSearchQuery(String corporateName) {
        LOG.info(ALPHABETICAL_SEARCH + "Building query for: " + corporateName);

        List<String> prefixes = generateCompanyNamePrefixes(corporateName);
        //prefixes.stream().forEach(System.out::println);

        // get alpha key value for corporate name
        AlphaKeyResponse alphaKeyResponse = alphaKeyService.getAlphaKeyForCorporateName(corporateName);
        String corporateNameAlphaKey = alphaKeyResponse.getSameAsAlphaKey();
        List<String> alphaPrefixes = generateCompanyNameAlphaKeyPrefixes(corporateNameAlphaKey);
        //alphaPrefixes.stream().forEach(System.out::println);

        // Strip company name ending. Corporate name search against corporate_name_start works best in lower case.
        String corporateNameSansEnding = stripCorporateEnding(corporateName).toLowerCase();
        String corporateNamePrevChar = appendPreviousCharacter(corporateNameSansEnding);
        String corporateNameAlphaPrefix = "", corporateNameAlphaPrevious = "";

        final int CORP_NAME_SANS_ENDING_LENGTH = corporateNameSansEnding.length();
        final int CORP_NAME_ALPHA_KEY_LENGTH = corporateNameAlphaKey.length();

        if (!corporateNameAlphaKey.isEmpty()) {
            if (CORP_NAME_ALPHA_KEY_LENGTH > 1) {
                corporateNameAlphaPrefix = corporateNameAlphaKey.substring(0, corporateNameAlphaKey.length() - 1);
                corporateNameAlphaPrevious = corporateNameAlphaPrefix + previousCharacter(corporateNameAlphaKey.charAt(corporateNameAlphaKey.length() - 1));
            } else {
                corporateNameAlphaPrevious = "" + previousCharacter(corporateNameAlphaKey.charAt(0));
            }
        }

        BoolQueryBuilder query = QueryBuilders.boolQuery();

        query.should(QueryBuilders.matchQuery("items.alpha_key", corporateNameAlphaKey).maxExpansions(100));
        query.should(QueryBuilders.queryStringQuery(corporateNamePrevChar));

        if (corporateNameSansEnding.contains(SPACE_CHARACTER)) {
            query.should(QueryBuilders.matchQuery("items.corporate_name_start.edge_ngram", corporateNameSansEnding)
                    .operator(Operator.AND).minimumShouldMatch("75%"));
            query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", corporateNameSansEnding + "a*"));
        }

        if (CORP_NAME_SANS_ENDING_LENGTH == 1 && !corporateNameSansEnding.matches("([\\s?&?$?0-9a-zA-Z])+")) {
            if (corporateNameSansEnding.equals("*")){
                corporateNameSansEnding = "\\*";
            }
            query.should(QueryBuilders.wildcardQuery("items.corporate_name_start", corporateNameSansEnding + "*"));
        }
        else if (CORP_NAME_SANS_ENDING_LENGTH == 1) {
            query.should(QueryBuilders.wildcardQuery("items.corporate_name_start", corporateNameSansEnding + "*"));
            query.should(QueryBuilders.wildcardQuery("items.corporate_name_start", corporateNameSansEnding + "*"));

            if (!corporateNameAlphaKey.isEmpty()) {
                query.should(QueryBuilders.matchQuery("items.alpha_key", corporateNameAlphaKey).minimumShouldMatch("75%"));
                query.should(QueryBuilders.matchQuery("items.alpha_key", corporateNameAlphaPrevious).minimumShouldMatch("75%"));
            }
        }
        else if (CORP_NAME_SANS_ENDING_LENGTH <= 3) {
            query.should(QueryBuilders.matchQuery("items.corporate_name_start.edge_ngram", corporateNameSansEnding));
        }
        else {
            for (int i = 0; i < prefixes.size(); i++) {
                if (corporateNameSansEnding.contains(SPACE_CHARACTER)) {
                    query.should(QueryBuilders.matchPhrasePrefixQuery("items.corporate_name_start", prefixes.get(i)));
                    query.should(QueryBuilders.matchQuery("items.corporate_name_start", prefixes.get(i)));
                    query.should(QueryBuilders.matchQuery("items.corporate_name_start", prefixes.get(i)));
                }
                query.should(QueryBuilders.queryStringQuery(prefixes.get(i)));
                query.should(QueryBuilders.wildcardQuery("items.corporate_name_start.edge_ngram", prefixes.get(i) + "*"));
            }
            for (int i = 0; i < alphaPrefixes.size(); i++) {
                query.should(QueryBuilders.queryStringQuery(alphaPrefixes.get(i) + "*"));
                query.should(QueryBuilders.wildcardQuery("items.alpha_key", alphaPrefixes.get(i) + "*"));
            }
            query.filter(QueryBuilders.regexpQuery("items.corporate_name_start", "~([.*])" + corporateNameSansEnding.charAt(0) + "([^ ]).*").boost(5));
        }
        //System.out.println(query.toString());

        return query;
    }
}
