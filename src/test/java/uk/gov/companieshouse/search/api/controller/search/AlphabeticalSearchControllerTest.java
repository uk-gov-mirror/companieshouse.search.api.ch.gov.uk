package uk.gov.companieshouse.search.api.controller.search;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import uk.gov.companieshouse.search.api.mapper.ApiToResponseMapper;
import uk.gov.companieshouse.search.api.model.SearchResults;
import uk.gov.companieshouse.search.api.model.esdatamodel.company.Items;
import uk.gov.companieshouse.search.api.model.response.ResponseObject;
import uk.gov.companieshouse.search.api.service.search.impl.alphabetical.AlphabeticalSearchIndexService;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.FOUND;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static uk.gov.companieshouse.search.api.model.response.ResponseStatus.SEARCH_FOUND;
import static uk.gov.companieshouse.search.api.model.response.ResponseStatus.SEARCH_NOT_FOUND;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AlphabeticalSearchControllerTest {

    @Mock
    private AlphabeticalSearchIndexService mockSearchIndexService;

    @Mock
    private ApiToResponseMapper mockApiToResponseMapper;

    @InjectMocks
    private AlphabeticalSearchController alphabeticalSearchController;

    private static final String REQUEST_ID = "requestID";
    private static final String COMPANY_NAME = "test name";

    @Test
    @DisplayName("Test search not found")
    void testSearchNotFound() {

        ResponseObject responseObject =
            new ResponseObject(SEARCH_NOT_FOUND, null);

        when(mockSearchIndexService.search(COMPANY_NAME, REQUEST_ID)).thenReturn(responseObject);
        when(mockApiToResponseMapper.map(responseObject))
            .thenReturn(ResponseEntity.status(NOT_FOUND).build());

        ResponseEntity responseEntity =
            alphabeticalSearchController.searchByCorporateName("test name", REQUEST_ID);

        assertNotNull(responseEntity);
        assertEquals(NOT_FOUND, responseEntity.getStatusCode());
    }

    @Test
    @DisplayName("Test search found")
     void testSearchFound() {

        ResponseObject responseObject =
            new ResponseObject(SEARCH_FOUND, createSearchResults());

        when(mockSearchIndexService.search(COMPANY_NAME, REQUEST_ID)).thenReturn(responseObject);
        when(mockApiToResponseMapper.map(responseObject))
            .thenReturn(ResponseEntity.status(FOUND).body(responseObject.getData()));

        ResponseEntity responseEntity =
            alphabeticalSearchController.searchByCorporateName("test name", REQUEST_ID);

        assertNotNull(responseEntity);
        assertEquals(FOUND, responseEntity.getStatusCode());
    }

    private SearchResults createSearchResults() {
        SearchResults<Items> searchResults = new SearchResults<>();
        List<Items> companies = new ArrayList<>();
        Items company = new Items();

        company.setCompanyNumber("00004444");
        company.setCorporateName("test name");
        company.setCompanyStatus("test status");
        companies.add(company);

        searchResults.setResults(companies);
        searchResults.setSearchType("test search type");

        return searchResults;
    }
}
