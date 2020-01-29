package uk.gov.companieshouse.search.api.service.search.impl.alphabetical;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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

import java.util.Arrays;

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

    private static final Logger LOG = LoggerFactory.getLogger(APPLICATION_NAME_SPACE);

    static final String SPACE_CHARACTER = " ";
    static final String CORPORATE_NAME_ENDINGS[] = {
            "PCC LTD",
            "PCC LIMITED",
            "PROTECTED CELL COMPANY",
            "+ COMPANY UNLTD",
            "AND COMPANY LLP",
            "AND COMPANY LTD",
            "AND COMPANY PLC",
            "COMPANY LIMITED",
            "LIMITED COMPANY",
            "& COMPANY UNLTD.",
            "AND CO UNLIMITED",
            "& COMPANY LIMITED",
            "+ COMPANY UNLTD",
            "AND COMPANY UNLTD",
            "& COMPANY UNLIMITED",
            "+ COMPANY LIMITED",
            "AND COMPANY LIMITED",
            "LIMITED PARTNERSHIP",
            "LIMITED PARTNERSHIPS",
            "AND COMPANY UNLIMITED",
            "COMMUNITY INTEREST PLC",
            "PUBLIC LIMITED COMPANY",
            "COMMUNITY INTEREST P.L.C",
            "CO PUBLIC LIMITED COMPANY",
            "AND PUBLIC LIMITED COMPANY",
            "COMMUNITY INTEREST COMPANY",
            "& CO PUBLIC LIMITED COMPANY",
            "+ CO PUBLIC LIMITED COMPANY",
            "AND CO PUBLIC LIMITED COMPANY",
            "LIMITED LIABILITY PARTNERSHIP",
            "OPEN-ENDED INVESTMENT COMPANY",
            "COMPANY PUBLIC LIMITED COMPANY",
            "& COMPANY PUBLIC LIMITED COMPANY",
            "+ COMPANY PUBLIC LIMITED COMPANY",
            "CO LIMITED LIABILITY PARTNERSHIP",
            "& CO LIMITED LIABILITY PARTNERSHIP",
            "+ CO LIMITED LIABILITY PARTNERSHIP",
            "AND COMPANY PUBLIC LIMITED COMPANY",
            "EUROPEAN ECONOMIC INTEREST GROUPING",
            "AND CO LIMITED LIABILITY PARTNERSHIP",
            "COMPANY LIMITED LIABILITY PARTNERSHIP",
            "& COMPANY LIMITED LIABILITY PARTNERSHIP",
            "CO",
            "LP",
            "CBP",
            "CIC",
            "LLP",
            "LTD",
            "PLC",
            "& CO",
            "+ CO",
            "EEIG",
            "EESV",
            "EOFG",
            "EOOS",
            "GEIE",
            "GELE",
            "ICVC",
            "LTD.",
            "NULL",
            "OEIC",
            "C.I.C",
            "P.L.C",
            "UNLTD",
            "AND CO",
            "CO LLP",
            "CO LTD",
            "CO PLC",
            "P.L.C.",
            "UNLTD.",
            "COMPANY",
            "LIMITED",
            "& CO LTD",
            "& CO PLC",
            "+ CO LTD",
            "+ CO PLC",
            "& COMPANY",
            "+ COMPANY",
            "UNLIMITED",
            "& CO UNLTD",
            "+ CO UNLTD",
            "AND CO LLP",
            "AND CO LTD",
            "AND CO PLC",
            "CO LIMITED",
            "& CO UNLTD.",
            "+ CO UNLTD.",
            "AND COMPANY",
            "COMPANY LLP",
            "COMPANY LTD",
            "COMPANY PLC",
            "& AND CO LLP",
            "& CO LIMITED",
            "+ CO LIMITED",
            "AND CO UNLTD",
            "& COMPANY LLP",
            "& COMPANY PLC",
            "+ COMPANY LLP",
            "+ COMPANY LTD",
            "+ COMPANY LIMITED LIABILITY PARTNERSHIP",
            "INVESTMENT COMPANY WITH VARIABLE CAPITAL",
            "AND COMPANY LIMITED LIABILITY PARTNERSHIP",
            "COMMUNITY INTEREST PUBLIC LIMITED COMPANY",
            "L.P.",
            "LIMITED.",
            ".LTD",
            "COMPANY LTD.",
            "INVALID ENDING",
            "& CO. LIMITED",
            "PARTNERSHIP",
            "CO.LIMITED",
            "CO. LIMITED",
            "CO.LTD",
            "& CO. LTD",
            "CO. LTD",
            "CO. LTD.",
            "CO.",
            "& CO.",
            "CO.",
            "AND CO. LTD.",
            "& CO. LTD.",
            "COMPANY",
            "CO.",
            "CO LTD.",
            "CO.",
            "AND CO. LIMITED",
            "AND CO. LTD",
            "+ COMPANY PLC",
            "AND CO UNLTD.",
            "& CO UNLIMITED",
            "+ CO UNLIMITED",
            "AND CO LIMITED",
            "& COMPANY UNLTD",
            "& COMPANY LTD",
            "& CO. LIMITED.",
            "AND CO.LIMITED",
            "& CO.LIMITED",
            "AND COMPANY LTD.",
            "& CO.",
            "& CO LLP",
            "+ CO. LIMITED",
            "& COMPANY",
            "& CO.",
            "+ CO. LTD",
            "AND CO LTD.",
            "& CO.",
            "AND CO.",
            "AND CO.",
            "AND CO.LTD",
            "& CO. LIMITED",
            "& COMPANY",
            "& CO.LTD",
            "+ CO LLP",
            "& CO",
            "AND COMPANY P.L.C.",
            "CO.",
            "COMPANY LIMITED",
            "& COMPANY",
            "CO",
            "& CO. LIMITED",
            "CO; LIMITED",
            "UN LIMITED",
            "P L C",
            "& CO. LIMITED",
            ".CO.",
            ".CO.",
            "C.I.C",
            "COMPANY. LIMITED",
            "& COMPANY. LIMITED",
            "AND COMPANY LIMITED",
            "COMPANY LIMITED",
            "COMPANY",
            "COMPANY LTD..",
            "& CO",
            "COMPANY P.L.C.",
            "COMPANY LIMITED.",
            "& COMPANY P.L.C.",
            "COMPANY P L C",
            "AND COMPANY LIMITED",
            "CO.LIMITED.",
            "COMPANY P.L.C.",
            "& CO.LIMITED.",
            "CO",
            "& CO. PUBLIC LIMITED COMPANY",
            "&CO. LIMITED",
            "& CO LIMITED",
            "& CO.LTD.",
            "& COMPANY LIMITED.",
            "CO LIMITED",
            "& COMPANY LIMITED",
            "& CO LIMITED",
            "CO. LIMITED",
            "CO.LTD.",
            "E.E.I.G.",
            "CO LTD",
            "COMPANY LTD",
            "P.L.C",
            ",LIMITED",
            "COMPANY P.L.C",
            "COMPANY",
            "CO.",
            "AND COMPANY",
            "COMPANY(THE)LIMITED",
            "CO",
            "COMPANYLIMITED",
            "CO",
            "CO.",
            "& CO.)LIMITED",
            "& CO.)LIMITED.",
            "COMPANY.LIMITED",
            "COMPANY",
            "COMPANY)LIMITED",
            "LTD.CO.",
            "& COMPANY.LIMITED",
            "THE)LIMITED",
            "&& CO.LIMITED",
            "COMPANY UN-LIMITED",
            "CO.PLC",
            "COMPANY",
            "AND COMPANY.LIMITED",
            "& CO;LIMITED",
            "CO",
            "& CO..LIMITED",
            "COMPANY",
            "AND COMPANYLIMITED",
            "CO",
            "COMPANY",
            "LTD",
            "PLC.",
            "& CO LTD.",
            "& COMPANY LTD.",
            "SE",
            "CCG CYF",
            "CCG CYFYNGEDIG",
            "CWMNI CELL GWARCHODEDIG",
            "AR CWMNI PAC",
            "AR CWMNI CYF",
            "AR CWMNI CCC",
            "CWMNI CYFYNGEDIG",
            "CYFYNGEDIG CWMNI",
            "& CWMNI CYFYNGEDIG",
            "+CWMNI CYFYNGEDIG",
            "AR CWMNI CYFYNGEDIG",
            "PARTNERIAETH CYFYNGEDIG",
            "CWMNI BUDDIANT CYMUNEDOL CCC",
            "CWMNI CYFYNGEDIG CYHOEDDUS",
            "CWMNI BUDDIANT CYMUNEDOL C.C.C",
            "AR CWMNI CYFYNGEDIG CYHOEDDUS",
            "CWMNI BUDDIANT CYMUNEDOL",
            "PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "CWMNIBUDDSODDIANTPENAGORED",
            "CWMNI CWMNI CYFYNGEDIG CYHOEDDUS",
            "&CWMNI CWMNI CYFYNGEDIG CYHOEDDUS",
            "+CWMNI CWMNI CYFYNGEDIG CYHOEDDUS",
            "CNI PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "& CNI PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "+ CNI PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "AR CWMNI CWMNI CYFYNGEDIG CYHOEDDUS",
            "AR CNI PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "CWMNI PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "& CWMNI PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "CBC",
            "PAC",
            "CYF",
            "CCC",
            "CBCN",
            "CYF.",
            "NULL",
            "C.B.C",
            "C.C.C",
            "CNI PAC",
            "C.C.C.",
            "CWMNI",
            "CYFYNGEDIG",
            "& CWMNI",
            "+ CWMNI",
            "ANGHYFYNGEDIG",
            "AR CNI PAC",
            "AR CWMNI",
            "CWMNI PAC",
            "CWMNI CYF",
            "CWMNI CCC",
            "& AR CNI PAC",
            "& CWMNI PAC",
            "& CWMNI CCC",
            "+ CWMNI PAC",
            "+ CWMNI CYF",
            "+ CWMNI PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "CWMNIBUDDSODDIACHYFALAFNEWIDIOL",
            "AR CWMNI PARTNERIAETH ATEBOLRWYDD CYFYNGEDIG",
            "CWMNI BUDDIANT CYMUNEDOL CYHOEDDUS CYFYNGEDIG",
            "+CWMNI CCC"
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public SearchRequest createSearchRequest(String corporateName, String requestId) {

        LOG.info(ALPHABETICAL_SEARCH + "Creating search request for: " + corporateName + " for user with Id: " + requestId);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(environmentReader.getMandatoryString(INDEX));
        searchRequest.preference(requestId);
        searchRequest.source(createSource(corporateName));

        return searchRequest;
    }

    private SearchSourceBuilder createSource(String corporateName) {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(Integer.parseInt(environmentReader.getMandatoryString(RESULTS_SIZE)));
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

    private String stripCompanyEnding(String corporateName){
        if (corporateName.contains(SPACE_CHARACTER) && Arrays.stream(CORPORATE_NAME_ENDINGS)
                .anyMatch((e) -> corporateName.toUpperCase().endsWith(e))){
            return corporateName.substring(0, corporateName.lastIndexOf(SPACE_CHARACTER));
        }
        return corporateName;
    }

    private char previousCharacter(char c){
        int cint = (int)c;
        if ((cint > 48 && cint < 57) || (cint > 65 && cint < 90) || (cint > 97 && cint < 122)) {
            return (char) (cint - 1);
        }
        if (cint == 65 || cint == 97){
            return '0';
        }
        else {
            return c;
        }
    }

    private QueryBuilder createAlphabeticalSearchQuery(String corporateName) {
        LOG.info(ALPHABETICAL_SEARCH + "Adding query for: " + corporateName);

        AlphaKeyResponse alphaKeyResponse = alphaKeyService.getAlphaKeyForCorporateName(corporateName);
        String corporateNameAlphaKey = alphaKeyResponse.getSameAsAlphaKey();
        String corporateNameOrderedAlphaKey = alphaKeyResponse.getOrderedAlphaKey();

        LOG.info("Alpha key for corporate: " + corporateName + " : " + corporateNameAlphaKey);
        LOG.info("Ordered alpha key for corporate: " + corporateName + " : " + corporateNameOrderedAlphaKey);

        String corporateNameSansEnding = stripCompanyEnding(corporateName).toLowerCase();
        String corporateNameFirstPart, corporateNamePrevious, corporateNamePrefix, corporateNameAlphaPrefix;
        if (corporateNameSansEnding.contains(SPACE_CHARACTER)) {
            corporateNameFirstPart = corporateNameSansEnding.substring(0, corporateNameSansEnding.indexOf(SPACE_CHARACTER));
        }
        else {
            corporateNameFirstPart = corporateNameSansEnding;
        }

        if (corporateNameFirstPart.length() > 1){
            corporateNamePrefix = corporateNameFirstPart.substring(0, corporateNameFirstPart.length()-1);
            char prevChar = previousCharacter(corporateNameFirstPart.charAt(corporateNameFirstPart.length()-1));
            corporateNamePrevious = corporateNamePrefix + prevChar;
        }
        else {
            corporateNamePrefix = corporateNameFirstPart;
            corporateNamePrevious = corporateNameFirstPart;
        }

        if (corporateNameAlphaKey.length() > 1) {
            corporateNameAlphaPrefix = corporateNameAlphaKey.substring(0, corporateNameAlphaKey.length()-1);
        }
        else {
            corporateNameAlphaPrefix = corporateNameAlphaKey;
        }

        BoolQueryBuilder query = QueryBuilders.boolQuery();
        // DisMaxQueryBuilder query = QueryBuilders.disMaxQuery();
        query.should(QueryBuilders.matchQuery("items.alpha_key", corporateNameAlphaKey).boost(5));
        query.should(QueryBuilders.prefixQuery("items.alpha_key", corporateNameAlphaKey).boost(5));
        query.should(QueryBuilders.wildcardQuery("items.alpha_key", corporateNameAlphaPrefix + "*"));
        query.should(QueryBuilders.regexpQuery("items.corporate_name_start", "^(" + corporateNamePrefix + ")([0-1])*([a-cA-C~l])*.*"));
        if (corporateNamePrefix.length() > 1) {
            query.should(QueryBuilders.matchQuery("items.corporate_name_start", corporateNamePrevious));
        }
        query.should(QueryBuilders.matchQuery("items.corporate_name_start", corporateNameFirstPart));
        //works
        //query.should(QueryBuilders.regexpQuery("items.corporate_name_start", "^(" + corporateNameFirstPart + ")([0-1])*([a-cA-C~l])*.*"));
        if (corporateNameFirstPart.length() > 1) {
            query.should(QueryBuilders.regexpQuery("items.corporate_name_start", "^(" + corporateNameFirstPart + "a)([0-1])*([a-cA-C~l])*.*"));
        }
        // works
        //query.filter(QueryBuilders.regexpQuery("items.corporate_name_start", "~(.+)" + corporateNameFirstPart + ".*"));
        query.filter(QueryBuilders.regexpQuery("items.corporate_name_start", "~([.*])" + corporateNamePrefix + "([^ ]).*"));

        return query;
    }
}
