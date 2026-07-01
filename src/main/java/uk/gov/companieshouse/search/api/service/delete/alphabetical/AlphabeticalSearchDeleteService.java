package uk.gov.companieshouse.search.api.service.delete.alphabetical;

import uk.gov.companieshouse.search.api.model.response.ResponseObject;

public interface AlphabeticalSearchDeleteService {

    ResponseObject<String> deleteCompany(String companyNumber);


}
