(config
  (password-field
    :name        "bearerToken"
    :label       "API Key"
    :placeholder "Enter the API Key")

  (text-array-field
    :name        "location"
    :label       "Location"
    :placeholder "Enter the locations with comma separated"
    :separator   ","))


(default-source
  (http/get :base-url "https://api.yelp.com/v3")
  (paging/no-pagination)
  (auth/http-bearer)
  (error-handler
    (when :status 429 :action rate-limit)))


(temp-entity BUSSINESS
  (api-docs-url "https://docs.developer.yelp.com/reference/v3_business_search")
  (source (http/get :url "/businesses/search"
            (query-params "location" "{location}"))
    (extract-path "")
    (setup-test
      (upon-receiving :code 200 :action (pass))))
  (fields
    id         :id))
