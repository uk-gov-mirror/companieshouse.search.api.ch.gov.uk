package uk.gov.companieshouse.search.api.service.search;

import org.springframework.http.ResponseEntity;
import uk.gov.companieshouse.search.api.model.alphakeyService.AlphaKeyResponse;

public interface AlphaKeyService {
    public AlphaKeyResponse getAlphaKeyForCorporateName(String corpName);
}
