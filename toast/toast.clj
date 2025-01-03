(global/table-select-and-resync-off)


(config
  (text-field
    :name        "subDomain"
    :label       "Domain"
    :placeholder "Enter your domain"
    (validation/regex "^[a-z0-9\\.\\-\\_]+$" :fail-message "Supports only [a-z0-9.-_] characters.")
    (api-config-field :masked false
      :api-field-name "subdomain"))

  (text-field
    :name "clientId"
    :label "Client ID"
    :placeholder "Enter your client ID"
    (api-config-field :masked false
      :api-field-name "client_id"))

  (password-field
    :name "clientSecret"
    :label "Client secret"
    :placeholder "Enter your client secret"
    (api-config-field :masked true
      :api-field-name "client_secret"))


  (oauth2/refresh-token-with-client-credentials
    (token
      (source
        (http/post
          :base-url ""
          :url "https://{subDomain}/authentication/v1/authentication/login"
          (header-params "Content-Type" "application/json")
          (body-params
            "clientId"     "{clientId}"
            "clientSecret" "{clientSecret}"
            "userAccessType" "TOAST_MACHINE_CLIENT")))
      (fields
        access_token :<= "token.accessToken"
        refresh_token :<= "token.refreshToken"
        token_type :<= "token.tokenType"
        scope      :<= "token.scope"
        realm_id   :<= "token.idToken"
        expires_in :<= "token.expiresIn"))))


(default-source
  (http/get :base-url "https://{subDomain}"
    (header-params "Accept" "application/json"))
  (paging/no-pagination)
  (auth/oauth2)
  (error-handler
    (when :status 429 :action (rate-limit :header-param-key "x-toast-ratelimit-reset" (timestamp/absolute (format "epoch-sec"))))
    (when :status 401 :action refresh)))


(entity RESTAURANT
  (api-docs-url "https://doc.toasttab.com/openapi/partners/operation/restaurantsGet/")
  (source
    (http/get :url "/partners/v1/restaurants")
    (setup-test
      (upon-receiving :code 200 :action (pass)))
    (extract-path "")
    (error-handler
      (when :status 429 :action rate-limit) ; in this endpoint we didn't have rate limit param in response header
      (when :status 401 :action refresh)))
  (fields
    id    :id :<= "restaurantGuid"
    name      :<= "restaurantName"
    management_group_guid      :<= "managementGroupGuid"
    location_name      :<= "locationName"
    created_by_email_address      :<= "createdByEmailAddress"
    external_group_ref      :<= "externalGroupRef"
    external_restaurant_ref      :<= "externalRestaurantRef"
    modified_date      :<= "modifiedDate"
    created_date      :<= "createdDate"
    iso_modified_date      :<= "isoModifiedDate"
    iso_created_date      :<= "isoCreatedDate"))


(entity JOB
  (api-docs-url "https://doc.toasttab.com/openapi/labor/operation/jobsGet/")
  (source
    (http/get :url "/labor/v1/jobs"
      (header-params "Accept" "application/json"
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (extract-path ""))
  (fields
    guid    :id :<= "guid"
    entity_type      :<= "entityType"
    external_id      :<= "externalId"
    created_date      :<= "createdDate"
    deleted
    code
    deleted_date      :<= "deletedDate"
    exclude_from_reporting      :<= "excludeFromReporting"
    modified_date      :<= "modifiedDate"
    tipped
    title
    default_wage      :<= "defaultWage"
    wage_frequency      :<= "wageFrequency")
  (sync-plan (delete-capture/upserts :when-prop "deleted" :is "true"))
  (relate
    (includes RESTAURANT :prop "id")))


(entity ORDERS
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (source (http/get :url "/orders/v2/ordersBulk"
            (header-params
              "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (extract-path "")
    (paging/page-number
      :page-number-query-param-initial-value 1
      :page-number-query-param-name "page"
      :limit 100
      :limit-query-param-name "pageSize"))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params
          "startDate" "$FROM"
          "endDate" "$TO")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (initial-value "2020-01-01T00:00:00Z")))
    (delete-capture/upserts :when-prop "deleted" :is "true"))
  (fields
    id   :id    :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId"
    created_in_test_mode 			:<=			 "createdInTestMode"
    source
    void_date 			:<=			 "voidDate"
    duration
    business_date 			:<=			 "businessDate"
    paid_date 			:<=			 "paidDate"
    excess_food 			:<=			 "excessFood"
    voided
    estimated_fulfillment_date 			:<=			 "estimatedFulfillmentDate"
    required_prep_time 			:<=			 "requiredPrepTime"
    approval_status 			:<=			 "approvalStatus"
    number_of_guest 			:<=			 "numberOfGuests"
    opened_date 			:<=			 "openedDate"
    void_business_date 			:<=			 "voidBusinessDate"
    deleted
    created_date 			:<=			 "createdDate"
    closed_date 			:<=			 "closedDate"
    deleted_date 			:<=			 "deletedDate"
    modified_date 			:<=			 "modifiedDate"
    promised_date 			:<=			 "promisedDate"
    channel_guid 		:<=			 "channelGuid"
    last_modified_device_id :<= "lastModifiedDevice.id"
    created_device_id :<= "createdDevice.id"
    guest_order_status  :<= "guestOrderStatus"
    initial_date  :<= "initialDate"
    void_service_charge  :<= "voidServiceCharges"
    capture_sequence_key  :<= "captureSequenceKey"
    facilitator_collect_and_remit_tax_order  :<= "marketplaceFacilitatorTaxInfo.facilitatorCollectAndRemitTaxOrder")
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "appliedPackagingInfo" :prefix "applied_packaging_info_")
    (flatten-fields
      (fields
        guid) :from "revenueCenter" :prefix "revenue_center_")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "owner")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        transport_color  :<= "transportColor"
        transport_description  :<= "transportDescription"
        note  :<= "notes") :from "curbsidePickupInfo"  :prefix "curbside_pickup_info_")
    (flatten-fields
      (fields
        guid
        entity_type  :<= "entityType"
        provider_id   :<= "providerId"
        provider_name   :<= "providerName"
        driver_name   :<= "driverName"
        driver_phone_number   :<= "driverPhoneNumber"
        provider_info   :<= "providerInfo"
        original_quoted_delivery_date   :<= "originalQuotedDeliveryDate") :from "deliveryServiceInfo"  :prefix "delivery_service_info_")
    (flatten-fields
      (fields
        guid) :from "deliveryInfo.deliveryEmployee" :prefix "delivery_employee_")
    (flatten-fields
      (fields
        address_1  :<= "address1"
        address_2  :<= "address2"
        city
        administrative_area  :<= "administrativeArea"
        state
        zip_code   :<= "zipCode"
        country
        latitude
        longitude
        note   :<= "notes"
        delivered_date   :<= "deliveredDate"
        dispatched_date  :<= "dispatchedDate"
        delivery_state   :<= "deliveryState") :from "deliveryInfo" :prefix "delivery_info_")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "server")
    (flatten-fields
      (fields
        guid) :from "serviceArea"  :prefix "service_area_")
    (flatten-fields
      (fields
        guid) :from "table")
    (flatten-fields
      (fields
        guid) :from "diningOption" :prefix "dining_option_")
    (flatten-fields
      (fields
        guid) :from "restaurantService" :prefix "restaurant_service_"))
  (relate
    (includes RESTAURANT :prop "id")
    (contains-list-of ORDERS_APPLIED_PACKING_ITEM :inside-prop "appliedPackagingInfo.appliedPackagingItems")
    (links-to RESTAURANT_SERVICE :prop "restaurant_service_guid")
    (links-to REVENUE_CENTER :prop "revenue_center_guid")
    (links-to SERVICE_AREA :prop "service_area_guid")
    (links-to DINING_OPTION :prop "dining_option_guid")
    (links-to TABLES :prop "table_guid")
    (links-to EMPLOYEE :prop "delivery_employee_guid")
    (contains-list-of ORDERS_CHECK :inside-prop "checks")
    (contains-list-of ORDERS_PRICING_FEATURE :inside-prop "pricingFeatures" :as pricing_feature)
    (contains-list-of ORDERS_MARKETPLACE_FACILITATOR_TAX_INFO :inside-prop "marketplaceFacilitatorTaxInfo.taxes")))


(entity ORDERS_APPLIED_PACKING_ITEM
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id   :id  :<= "guid"
    entity_type 			:<=			 "entityType"
    item_config_id  :<= "itemConfigId"
    inclusion
    guest_display_name   :<= "guestDisplayName")
  (relate
    (needs ORDERS :prop "id")
    (contains-list-of ORDERS_APPLIED_PACKING_ITEM_TYPE :inside-prop "itemTypes" :as item_type)))


(entity ORDERS_APPLIED_PACKING_ITEM_TYPE
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    index :id :index
    item_type)
  (relate
    (needs ORDERS_APPLIED_PACKING_ITEM :prop "id")
    (needs ORDERS_APPLIED_PACKING_ITEM :prop "orders_id" :as orders_id)))


(entity ORDERS_CHECK
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id  		:id			:<= "guid"
    display_number   :<= "displayNumber"
    payment_status 			:<=			 "paymentStatus"
    amount
    tab_name 			:<=			 "tabName"
    tax_exempt 			:<=			 "taxExempt"
    total_amount 			:<=			 "totalAmount"
    closed_date 			:<=			 "closedDate"
    tax_amount 			:<=			 "taxAmount"
    net_amount  :<= "netAmount"
    total_discount_amount   :<= "totalDiscountAmount"
    picked_up_date  :<= "pickedUpDate"
    tip_amount   :<= "tipAmount"
    entity_type 			:<=			 "entityType"
    external_id 	:<=			 "externalId"
    duration
    paid_date 			:<=			 "paidDate"
    opened_date 			:<=			 "openedDate"
    created_date 			:<=			 "createdDate"
    deleted
    deleted_date 			:<=			 "deletedDate"
    modified_date 			:<=			 "modifiedDate"
    last_modified_device_id 	:<=	"lastModifiedDevice.id"
    created_device_id 	:<=	"createdDevice.id"
    voided
    void_date     :<= "voidDate"
    void_business_date  :<= "voidBusinessDate")
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        loyalty_identifier   :<= "loyaltyIdentifier"
        masked_loyalty_identifier  :<= "maskedLoyaltyIdentifier"
        vendor
        accrual_family_guid  :<= "accrualFamilyGuid"
        accrual_text  :<= "accrualText") :from "appliedLoyaltyInfo"  :prefix "applied_loyalty_info_")
    (flatten-fields
      (fields
        guid
        one_time_use  :<= "oneTimeUse"
        pre_auth_amount  :<= "preAuthAmount"
        card_type   :<= "cardType"
        reader_type  :<= "readerType"
        card_holder_first_name   :<= "cardHolderFirstName"
        card_holder_last_name   :<= "cardHolderLastName"
        mag_stripe_name   :<= "magStripeName"
        card_holder_expiration_month  :<= "cardHolderExpirationMonth"
        card_holder_expiration_year   :<= "cardHolderExpirationYear"
        use_count   :<= "useCount") :from "appliedPreauthInfo"  :prefix "applied_preauth_info_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        first_name   :<= "firstName"
        last_name  :<= "lastName"
        phone
        phone_token   :<= "phoneToken"
        phone_country_code  :<= "phoneCountryCode"
        email
        email_token   :<= "emailToken") :from "billingCustomer"  :prefix "billing_customer_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        first_name   :<= "firstName"
        last_name  :<= "lastName"
        phone
        phone_token   :<= "phoneToken"
        phone_country_code  :<= "phoneCountryCode"
        email
        email_token   :<= "emailToken") :from "customer")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        external_id  :<= "externalId") :from "driverShift"  :prefix "driver_shift_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType") :from "guestProfile"  :prefix "guest_profile_")
    (flatten-fields
      (fields
        state
        number) :from "taxExemptionAccount"  :prefix "tax_exemption_account_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        external_id  :<= "externalId") :from "shift"))
  (relate
    (contains-list-of ORDERS_CHECK_APPLIED_DISCOUNT :inside-prop "appliedDiscounts")
    (contains-list-of ORDERS_CHECK_APPLIED_SERVICE_CHARGE :inside-prop "appliedServiceCharges")
    (contains-list-of ORDERS_CHECK_PAYMENT :inside-prop "payments")
    (contains-list-of ORDERS_CHECK_SELECTION :inside-prop "selections")
    (contains-list-of ORDERS_CHECK_REMOVED_SELECTION :inside-prop "removedSelections")
    (needs ORDERS :prop "id")))


(entity ORDERS_CHECK_APPLIED_DISCOUNT
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id  		:id  :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId"
    applied_promo_code  :<= "appliedPromoCode"
    discount_amount  :<= "discountAmount"
    discount_percent  :<= "discountPercent"
    discount_type  :<= "discountType"
    name
    non_tax_discount_amount  :<= "nonTaxDiscountAmount"
    processing_state  :<= "processingState")
  (dynamic-fields
    (flatten-fields
      (fields
        name
        description
        comment
        discount_reason_guid  :<= "discountReason.guid"
        discount_reason_entity_type  :<= "discountReason.entityType") :from "appliedDiscountReason" :prefix "applied_discount_reason_")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 		:<=			 "externalId") :from "approver")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "discount")
    (flatten-fields
      (fields
        vendor
        reference_id 			:<=			 "referenceId") :from "loyaltyDetails"  :prefix "loyalty_details_"))
  (relate
    (includes ORDERS_CHECK :prop "id")
    (links-to DISCOUNTS :prop "discount_guid")
    (contains-list-of ORDERS_CHECK_APPLIED_DISCOUNT_COMBO_ITEM :inside-prop "comboItems")
    (contains-list-of ORDERS_CHECK_APPLIED_DISCOUNT_TRIGGER :inside-prop "triggers")))


(entity ORDERS_CHECK_APPLIED_DISCOUNT_TRIGGER
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    index :id :index
    quantity)
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 		:<=			 "externalId") :from "selection"))
  (relate
    (needs ORDERS_CHECK_APPLIED_DISCOUNT :prop "id")))


(entity ORDERS_CHECK_APPLIED_DISCOUNT_COMBO_ITEM
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id 		    :id     :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		  :<=			 "externalId")
  (relate
    (includes ORDERS_CHECK_APPLIED_DISCOUNT :prop "id" :as orders_applied_discount_id)))


(entity ORDERS_CHECK_APPLIED_SERVICE_CHARGE
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id :id :<= "guid"
    charge_amount  :<= "chargeAmount"
    charge_type  :<= "chargeType"
    delivery
    destination
    dine_in  :<= "dineIn"
    entity_type  :<= "entityType"
    external_id  :<= "externalId"
    gratuity
    name
    payment_guid  :<= "paymentGuid"
    percents    :<= "percent"
    service_charge_calculation  :<= "serviceChargeCalculation"
    service_charge_category  :<= "serviceChargeCategory"
    tags  (json)
    takeout
    taxable)
  (dynamic-fields
    (flatten-fields
      (fields
        refund_amount  :<= "refundAmount"
        tax_refund_amount  :<= "taxRefundAmount"
        refund_transaction_guid  :<= "refundTransaction.guid"
        refund_transaction_entity_type  :<= "refundTransaction.entityType")
      :from "refundDetails" :prefix "")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 		:<=			 "externalId") :from "serviceCharge" :prefix "service_charge_"))
  (relate
    (needs ORDERS_CHECK :prop "id")
    (contains-list-of ORDERS_CHECK_APPLIED_SERVICE_CHARGE_APPLIED_TAX :inside-prop "appliedTaxes")))


(entity ORDERS_CHECK_APPLIED_SERVICE_CHARGE_APPLIED_TAX
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id 		:id  :<= "guid"
    entity_type 			:<=			 "entityType"
    facilitator_collect_and_remit_tax  :<= "facilitatorCollectAndRemitTax"
    name
    rate
    tax_amount  :<= "taxAmount"
    type)
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "taxRate" :prefix "tax_rate_"))
  (relate
    (includes ORDERS_CHECK_APPLIED_SERVICE_CHARGE :prop "id")))


(entity ORDERS_CHECK_PAYMENT
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    payment_id :id :<= "guid"
    order_guid  :<= "orderGuid")
  (relate
    (needs ORDERS_CHECK :prop "id")
    (links-to PAYMENT :prop "payment_id")
    (links-to ORDERS :prop "order_guid")))


(entity ORDERS_CHECK_REMOVED_SELECTION
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id :id :<= "guid"
    entity_type  :<= "entityType"
    price
    quantity)
  (relate
    (needs ORDERS_CHECK :prop "id")))


(entity ORDERS_CHECK_SELECTION
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id 		:id			:<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId"
    deferred
    pre_discount_price 			:<=			 "preDiscountPrice"
    void_business_date    :<= "voidBusinessDate"
    voided
    display_name 			:<=			 "displayName"
    seat_number   :<= "seatNumber"
    void_date 			:<=			 "voidDate"
    fulfillment_status 			:<=			 "fulfillmentStatus"
    option_group_pricing_mode 			:<=			 "optionGroupPricingMode"
    selection_type 			:<=			 "selectionType"
    price
    stored_value_transaction_id 		:<=			 "storedValueTransactionId"
    tax_inclusion 			:<=			 "taxInclusion"
    quantity
    receipt_line_price 			:<=			 "receiptLinePrice"
    unit_of_measure 			:<=			 "unitOfMeasure"
    tax
    created_date 			:<=			 "createdDate"
    modified_date 			:<=			 "modifiedDate"
    open_price_amount  :<= "openPriceAmount"
    external_price_amount  :<= "externalPriceAmount")
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        multi_location_id  :<= "multiLocationId"
        external_id  :<= "externalId") :from "item")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "diningOption" :prefix "dining_option_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        multi_location_id  :<= "multiLocationId"
        external_id  :<= "externalId") :from "itemGroup" :prefix "item_group_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        multi_location_id  :<= "multiLocationId"
        external_id  :<= "externalId") :from "optionGroup" :prefix "option_group_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        external_id  :<= "externalId") :from "preModifier" :prefix "pre_modifier_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        external_id  :<= "externalId") :from "salesCategory" :prefix "sales_category_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        external_id  :<= "externalId") :from "voidReason" :prefix "void_reason_")
    (flatten-fields
      (fields
        refund_amount    :<= "refundAmount"
        tax_refund_amount  :<= "taxRefundAmount"
        refund_transaction_guid    :<= "refundTransaction.guid"
        refund_transaction_entity_type    :<= "refundTransaction.entityType") :from "refundDetails" :prefix "")
    (flatten-fields
      (fields
        from     :<= "giftCardMessage.from"
        to     :<= "giftCardMessage.to"
        message     :<= "giftCardMessage.message"
        email     :<= "giftCardMessage.email"
        phone     :<= "giftCardMessage.phone"
        purchaser_email     :<= "giftCardMessage.purchaserEmail"
        purchaser_phone     :<= "giftCardMessage.purchaserPhone"
        requested_delivery_date     :<= "giftCardMessage.requestedDeliveryDate"
        guid     :<= "giftCardMessage.guid"
        amount) :from "giftCardSelectionInfo" :prefix "gift_card_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType") :from "toastGiftCard" :prefix "toast_gift_card_"))
  (relate
    (needs ORDERS_CHECK :prop "id")
    (contains-list-of ORDERS_CHECK_SELECTION_APPLIED_DISCOUNT :inside-prop "appliedDiscounts")
    (contains-list-of ORDERS_CHECK_SELECTION_APPLIED_TAX :inside-prop "appliedTaxes")
    (contains-list-of ORDERS_CHECK_SELECTION_MODIFIER :inside-prop "modifiers")))


(entity ORDERS_CHECK_SELECTION_APPLIED_DISCOUNT
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id  		:id  :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId"
    applied_promo_code  :<= "appliedPromoCode"
    discount_amount  :<= "discountAmount"
    discount_percent  :<= "discountPercent"
    discount_type  :<= "discountType"
    name
    non_tax_discount_amount  :<= "nonTaxDiscountAmount"
    processing_state  :<= "processingState")
  (dynamic-fields
    (flatten-fields
      (fields
        name
        description
        comment
        discount_reason_guid  :<= "discountReason.guid"
        discount_reason_entity_type  :<= "discountReason.entityType") :from "appliedDiscountReason" :prefix "applied_discount_reason_")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 		:<=			 "externalId") :from "approver")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "discount")
    (flatten-fields
      (fields
        vendor
        reference_id 			:<=			 "referenceId") :from "loyaltyDetails"  :prefix "loyalty_details_"))
  (relate
    (includes ORDERS_CHECK_SELECTION :prop "id")
    (links-to DISCOUNTS :prop "discount_guid")
    (contains-list-of ORDERS_CHECK_SELECTION_APPLIED_DISCOUNT_COMBO_ITEM :inside-prop "comboItems")
    (contains-list-of ORDERS_CHECK_SELECTION_APPLIED_DISCOUNT_TRIGGER :inside-prop "triggers")))


(entity ORDERS_CHECK_SELECTION_APPLIED_DISCOUNT_TRIGGER
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    quantity)
  (dynamic-fields
    (fivetran-id)
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 		:<=			 "externalId") :from "selection"))
  (relate
    (includes ORDERS_CHECK_SELECTION_APPLIED_DISCOUNT :prop "id")))


(entity ORDERS_CHECK_SELECTION_APPLIED_DISCOUNT_COMBO_ITEM
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id 		:id       :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId")
  (relate
    (includes ORDERS_CHECK_SELECTION_APPLIED_DISCOUNT :prop "id" :as orders_applied_discount_id)))


(entity ORDERS_CHECK_SELECTION_APPLIED_TAX
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    index :id :index
    id 		  :<= "guid"
    entity_type 			:<=			 "entityType"
    facilitator_collect_and_remit_tax  :<= "facilitatorCollectAndRemitTax"
    name
    rate
    tax_amount  :<= "taxAmount"
    type)
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "taxRate" :prefix "tax_rate_"))
  (relate
    (needs ORDERS_CHECK_SELECTION :prop "id")))


(entity ORDERS_CHECK_SELECTION_MODIFIER
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id  :id :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId"
    deferred
    pre_discount_price 			:<=			 "preDiscountPrice"
    display_name 			:<=			 "displayName"
    applied_discount    :<= "appliedDiscounts"  (json)
    modifier    :<= "modifiers"  (json)
    seat_number   :<= "seatNumber"
    fulfillment_status 			:<=			 "fulfillmentStatus"
    option_group_pricing_mode 			:<=			 "optionGroupPricingMode"
    sales_category 			:<=			 "salesCategory"
    selection_type 			:<=			 "selectionType"
    price
    applied_tax   :<= "appliedTaxes"  (json)
    stored_value_transaction_id 		:<=			 "storedValueTransactionId"
    item_group     :<= "itemGroup"
    tax_inclusion 			:<=			 "taxInclusion"
    quantity
    receipt_line_price 			:<=			 "receiptLinePrice"
    unit_of_measure 			:<=			 "unitOfMeasure"
    refund_detail 			:<=			 "refundDetails"
    toast_gift_card 			:<=			 "toastGiftCard"
    tax
    dining_option   :<= "diningOption"
    created_date 			:<=			 "createdDate"
    pre_modifier 			:<=			 "preModifier"
    modified_date 			:<=			 "modifiedDate")
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        multi_location_id  :<= "multiLocationId"
        external_id  :<= "externalId") :from "optionGroup" :prefix "option_group_")
    (flatten-fields
      (fields
        guid
        entity_type   :<= "entityType"
        multi_location_id  :<= "multiLocationId"
        external_id  :<= "externalId") :from "item"))
  (relate
    (needs ORDERS_CHECK_SELECTION :prop "id")))


(entity ORDERS_PRICING_FEATURE
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    index :id :index
    pricing_feature)
  (relate
    (needs ORDERS :prop "id")))


(entity ORDERS_MARKETPLACE_FACILITATOR_TAX_INFO
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id 		:id  :<= "guid"
    entity_type 			:<=			 "entityType"
    name
    rate
    tax_amount  :<= "taxAmount"
    type
    facilitator_collect_and_remit_tax  :<= "facilitatorCollectAndRemitTax"
    tax_rate_guid :<= "taxRate.guid"
    tax_rate_entity_type :<= "taxRate.entityType")
  (relate
    (includes ORDERS :prop "id")))


(entity REVENUE_CENTER
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/revenueCentersGet/")
  (source
    (http/get :url "/config/v2/revenueCenters"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (step-size "30 d"))))
  (fields
    id 		:id  :<= "guid"
    entity_type 			:<=			 "entityType"
    name
    description)
  (relate
    (includes RESTAURANT :prop "id")))


(entity RESTAURANT_SERVICE
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/restaurantServicesGet/")
  (source
    (http/get :url "/config/v2/restaurantServices"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (step-size "30 d"))))
  (fields
    id 		:id  :<= "guid"
    entity_type 			:<=			 "entityType"
    name)
  (relate
    (includes RESTAURANT :prop "id")))


(entity SERVICE_AREA
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/serviceAreasGet/")
  (source
    (http/get :url "/config/v2/serviceAreas"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (step-size "30 d"))))
  (fields
    id 		:id			:<= "guid"
    entity_type 			:<=			 "entityType"
    name)
  (dynamic-fields
    (flatten-fields
      (fields
        guid) :from "revenueCenter" :prefix "revenue_center_"))
  (relate
    (includes RESTAURANT :prop "id")
    (links-to REVENUE_CENTER :prop "revenue_center_guid")))


(temp-entity TEMP_DINING_OPTION
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/diningOptionsGet/")
  (source
    (http/get :url "/config/v2/diningOptions"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (step-size "30 d"))))
  (fields
    id 		:id       :<= "guid"    (transformation (text/replace-null "null"))
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId"
    name
    curbside
    behavior)
  (relate
    (includes RESTAURANT :prop "id")))


(entity DINING_OPTION
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/diningOptionsGet/")
  (fields
    id 		:id       :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId"
    name
    curbside
    behavior)
  (relate
    (is TEMP_DINING_OPTION (whose id  :is-not "null"))
    (includes RESTAURANT :prop "id")))


(temp-entity TEMP_TABLES
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/tablesGet/")
  (source
    (http/get :url "/config/v2/tables"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (step-size "30 d"))))
  (fields
    id 		:id       :<= "guid"    (transformation (text/replace-null "null"))
    entity_type 			:<=			 "entityType"
    name
    service_area_guid  :<= "serviceArea.guid"
    revenue_center_guid  :<= "revenueCenter.guid")
  (relate
    (includes RESTAURANT :prop "id")))


(entity TABLES
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/tablesGet/")
  (fields
    id 		:id       :<= "guid"
    entity_type 			:<=			 "entityType"
    name
    service_area_guid  :<= "serviceArea.guid"
    revenue_center_guid  :<= "revenueCenter.guid")
  (relate
    (is TEMP_TABLES (whose id  :is-not "null"))
    (includes RESTAURANT :prop "id")
    (links-to SERVICE_AREA :prop "service_area_guid")
    (links-to REVENUE_CENTER :prop "revenue_center_guid")))


;; facing 403 forbidden error  due to scopes // No-pagination & no-sync
(entity EMPLOYEE
  (api-docs-url "https://doc.toasttab.com/openapi/labor/operation/employeesGet/")
  (source
    (http/get :url "/labor/v1/employees"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (extract-path "")
    (error-handler
      (when :status 429 :action (rate-limit :header-param-key "x-ratelimit-reset" (timestamp/relative (format "epoch-sec"))))
      (when :status 401 :action refresh)
      (when :status 403 :action (skip :max-retries 0))))
  (fields
    id 		:id  :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id  :<= "externalId"
    created_date  :<= "createdDate"
    modified_date  :<= "modifiedDate"
    deleted_date   :<= "deletedDate"
    first_name  :<= "firstName"
    chosen_name   :<= "chosenName"
    last_name   :<= "lastName"
    email
    phone_number   :<= "phoneNumber"
    phone_number_country_code  :<= "phoneNumberCountryCode"
    passcode
    external_employee_id  :<= "externalEmployeeId"
    deleted
    v2_employee_guid  :<= "v2EmployeeGuid")
  (sync-plan (delete-capture/upserts :when-prop "deleted" :is "true"))
  (relate
    (includes RESTAURANT :prop "id")
    (contains-list-of EMPLOYEE_JOB_REFERENCE :inside-prop "jobReferences")
    (contains-list-of EMPLOYEE_WAGE_OVERRIDE :inside-prop "wageOverrides")))


(entity EMPLOYEE_JOB_REFERENCE
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    id  :id  :<= "guid"
    entity_type   :<= "entityType"
    external_id   :<= "externalId")
  (relate
    (needs EMPLOYEE :prop "id")))


(entity EMPLOYEE_WAGE_OVERRIDE
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/")
  (fields
    wage
    id  :id  :<= "jobReference.guid"
    entity_type :<= "jobReference.guid"
    external_id :<= "jobReference.guid")
  (relate
    (needs EMPLOYEE :prop "id")))


(entity SHIFT
  (api-docs-url "https://doc.toasttab.com/openapi/labor/operation/shiftsGet/")
  (source
    (http/get :url "/labor/v1/shifts"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params
          "startDate" "$FROM"
          "endDate" "$TO")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (initial-value "2020-01-01T00:00:00Z")
        (step-size "30 d")))
    (delete-capture/upserts :when-prop "deleted" :is "true"))
  (fields
    id  :id   :<= "guid"
    created_date :<= "createdDate"
    deleted
    deleted_date :<= "deletedDate"
    modified_date :<= "modifiedDate"
    out_date :<= "outDate"
    in_date :<= "inDate")
  (dynamic-fields
    (flatten-fields
      (fields
        id :<= "guid") :from "jobReference"  :prefix "job_reference_")
    (flatten-fields
      (fields
        id :<= "guid") :from "employeeReference" :prefix "employee_reference_")
    (flatten-fields
      (fields
        id :<= "guid"
        min_after_clock_in :<= "minAfterClockIn"
        min_after_clock_out :<= "minAfterClockOut"
        min_before_clock_in :<= "minBeforeClockIn"
        min_before_clock_out :<= "minBeforeClockOut") :from "scheduleConfig" :prefix "schedule_config_"))
  (relate
    (includes RESTAURANT :prop "id")
    (links-to EMPLOYEE_JOB_REFERENCE.id :prop "job_reference_id")
    (links-to JOB :prop "job_reference_id")
    (links-to EMPLOYEE :prop "employee_reference_id")))


(entity TIME_ENTRY
  (api-docs-url "https://doc.toasttab.com/openapi/labor/operation/timeEntriesGet/")
  (source
    (http/get :url "/labor/v1/timeEntries"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params
          "modifiedStartDate" "$FROM"
          "modifiedEndDate" "$TO")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (initial-value "2020-01-01T00:00:00Z")
        (step-size "30 d")))
    (delete-capture/upserts :when-prop "deleted" :is "true"))
  (fields
    id :id :<= "guid"
    auto_clocked_out :<= "autoClockedOut"
    business_date :<= "businessDate"
    cash_gratuity_service_charges :<= "cashGratuityServiceCharges"
    cash_sales :<= "cashSales"
    created_date :<= "createdDate"
    declared_cash_tips  :<= "declaredCashTips"
    deleted
    deleted_date :<= "deletedDate"
    hourly_wage :<= "hourlyWage"
    in_date :<= "inDate"
    modified_date :<= "modifiedDate"
    non_cash_gratuity_service_charges :<= "nonCashGratuityServiceCharges"
    non_cash_sales :<= "nonCashSales"
    non_cash_tips :<= "nonCashTips"
    regular_hours :<= "regularHours"
    tips_withheld :<= "tipsWithheld"
    out_date :<= "outDate"
    overtime_hours :<= "overtimeHours")
  (dynamic-fields
    (flatten-fields
      (fields
        id :<= "guid") :from "jobReference"  :prefix "job_reference_")
    (flatten-fields
      (fields
        id :<= "guid") :from "employeeReference" :prefix "employee_reference_")
    (flatten-fields
      (fields
        id :<= "guid") :from "shiftReference" :prefix "shift_reference_"))
  (relate
    (includes RESTAURANT :prop "id")
    (links-to EMPLOYEE_JOB_REFERENCE.id :prop "job_reference_id")
    (links-to EMPLOYEE :prop "employee_reference_id")
    (links-to JOB :prop "job_reference_id")
    (contains-list-of BREAK :inside-prop "breaks")))


(entity BREAK
  (api-docs-url "https://doc.toasttab.com/openapi/labor/operation/timeEntriesGet/")
  (fields
    id :id :<= "guid"
    audit_response :<= "auditResponse"
    missed)
  (dynamic-fields
    (flatten-fields
      (fields
        id :<= "guid"
        entity_type :<= "entityType")  :from "breakType" :prefix "break_type_"))
  (relate
    (needs TIME_ENTRY :prop "id")))


(entity CASH_ENTRY
  (api-docs-url "https://doc.toasttab.com/openapi/cashmanagement/operation/entriesGet/")
  (source
    (http/get :url "/cashmgmt/v1/entries"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "businessDate" "$FROM")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyyMMdd"))))
  (fields
    id 		:id       :<= "guid"
    entity_type 			:<=			 "entityType"
    amount
    reason
    date
    type
    undoes)
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "cashDrawer")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "payoutReason")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "noSaleReason")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "employee1" :prefix "employee_1_")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "employee2" :prefix "employee_2_")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "creatorOrShiftReviewSubject" :prefix "creator_")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "approverOrShiftReviewSubject" :prefix "approver_"))
  (relate
    (includes RESTAURANT :prop "id")
    (links-to EMPLOYEE :prop "employee_1_guid")
    (links-to EMPLOYEE :prop "employee_2_guid")
    (links-to EMPLOYEE :prop "creator_guid")
    (links-to EMPLOYEE :prop "approver_guid")))


(entity CASH_DEPOSIT
  (api-docs-url "https://doc.toasttab.com/openapi/cashmanagement/operation/depositsGet/")
  (source
    (http/get :url "/cashmgmt/v1/deposits"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "businessDate" "$FROM")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyyMMdd"))))
  (fields
    id 		:id       :<= "guid"
    entity_type 			:<=			 "entityType"
    amount
    date
    undoes)
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "employee")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "creator"))
  (relate
    (includes RESTAURANT :prop "id")
    (links-to EMPLOYEE :prop "employee_guid")
    (links-to EMPLOYEE :prop "creator_guid")))


(temp-entity TEMP_DISCOUNTS
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/discountsGet/")
  (source
    (http/get :url "/config/v2/discounts"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (step-size "30 d"))))
  (fields
    id          :id     :<= "guid"  (transformation (text/replace-null "null"))
    entity_type 			  :<=			 "entityType"
    amount
    selection_type 			:<=			 "selectionType"
    non_exclusive 			:<=			 "nonExclusive"
    percentage
    name
    active
    item_picking_priority 			:<=			 "itemPickingPriority"
    type
    fixed_total 			          :<=			 "fixedTotal")
  (relate
    (includes RESTAURANT :prop "id")))


(entity DISCOUNTS
  (fields
    id  :id  :<= "guid"
    entity_type 	:<=			 "entityType"
    amount
    selection_type 			:<=			 "selectionType"
    non_exclusive 			  :<=			 "nonExclusive"
    percentage
    name
    active
    item_picking_priority 			:<=			 "itemPickingPriority"
    type
    fixed_total 			:<=			 "fixedTotal")
  (relate
    (is TEMP_DISCOUNTS (whose id  :is-not "null"))
    (includes RESTAURANT :prop "id")))


(entity MENU_ITEM
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/menuItemsGet/")
  (source
    (http/get :url "/config/v2/menuItems"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2024-02-20T00:00:00Z") ; Decide on a date for this table after consulting with TPS.
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSz")
        (step-size "30 d"))))
  (fields
    id 		     :id       :<=      "guid"
    entity_type 			:<=			 "entityType"
    external_id 	  	:<=			 "externalId"
    images 		                        (json)
    visibility
    unit_of_measure    :<=			 "unitOfMeasure"
    calories
    type
    inherit_unit_of_measure 			:<=			 "inheritUnitOfMeasure"
    inherit_option_groups 			:<=			 "inheritOptionGroups"
    orderable_online 			:<=			 "orderableOnline"
    name
    plu
    sku)
  (relate
    (includes RESTAURANT :prop "id")))


(entity MENU
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/menusGet/")
  (source
    (http/get :url "/config/v2/menus"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2024-01-01T00:00:00Z") ; Decide on a date for this table after consulting with TPS.
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSz")
        (step-size "30 d"))))
  (fields
    id 		 :id      :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId"
    images 		                   (json)
    visibility
    unit_of_measure 			:<=			 "unitOfMeasure"
    orderable_online 			:<=			 "orderableOnline"
    name)
  (relate
    (includes RESTAURANT :prop "id")
    (contains-list-of MENU_GROUP :inside-prop "groups")))


(entity MENU_GROUP
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/menusGet/")
  (fields
    id 		:id       :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 		:<=			 "externalId")
  (relate
    (needs MENU :prop "id")))


(temp-entity TEMP_SALE_CATEGORY
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/salesCategoriesGet/")
  (source
    (http/get :url "/config/v2/salesCategories"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (paging/key-based
      (next-page-source/header)
      :scroll-key-query-param-name "toast-next-page-token"
      :scroll-value-path-in-response "pageToken")
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "lastModified" "$FROM" "dummy" "$TO")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        (step-size "30 d"))))
  (fields
    id 		:id       :<= "guid"   (transformation (text/replace-null "null"))
    entity_type 			:<=			 "entityType"
    name)
  (relate
    (includes RESTAURANT :prop "id")))


(entity SALE_CATEGORY
  (api-docs-url "https://doc.toasttab.com/openapi/configuration/operation/salesCategoriesGet/")
  (fields
    id 		:id       :<= "guid"
    entity_type 			:<=			 "entityType"
    name)
  (relate
    (is TEMP_SALE_CATEGORY (whose id  :is-not "null"))
    (includes RESTAURANT :prop "id")))


(temp-entity PAYMENT_ID
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/paymentsGet/")
  (source
    (http/get :url "/orders/v2/payments"
      (header-params
        "Toast-Restaurant-External-ID" "{RESTAURANT.id}"))
    (response/transformation (java-class))
    (extract-path ""))
  (sync-plan
    (change-capture-cursor
      (subset/by-time
        (query-params "paidBusinessDate" "$FROM")
        (initial-value "2020-01-01T00:00:00Z")
        (format "yyyyMMdd"))))
  (relate
    (contains-list-of PAYMENT_ID_TEMP :inside-prop "payment_id" :as e_id)))


(temp-entity PAYMENT_ID_TEMP
  (fields
    e_id :id)
  (relate
    (includes RESTAURANT :prop "id" :as restaurant_id)))


(entity PAYMENT
  (api-docs-url "https://doc.toasttab.com/openapi/orders/operation/paymentsGuidGet/")
  (source
    (http/get :url "/orders/v2/payments/{PAYMENT_ID_TEMP.e_id}"
      (header-params
        "Toast-Restaurant-External-ID" "{PAYMENT_ID_TEMP.restaurant_id}"))
    (extract-path ""))
  (fields
    id            :id   :<= "guid"
    entity_type 			:<=			 "entityType"
    external_id 			:<=			 "externalId"
    paid_date 					:<=			 "paidDate"
    paid_business_date 			:<=			 "paidBusinessDate"
    type 						:<=			 "type"
    card_entry_mode 				:<=			 "cardEntryMode"
    amount 						:<=			 "amount"
    tip_amount 					:<=			 "tipAmount"
    amount_tendered 				:<=			 "amountTendered"
    card_type 					:<=			 "cardType"
    original_processing_fee 		:<=			 "originalProcessingFee"
    order_owner_guid 				:<=			 "orderOwnerGuid"
    card_processor_type 			:<=			 "cardProcessorType"
    payment_method_id 			:<=			 "paymentMethodId"
    tender_room_id 				:<=			 "tenderRoomId"
    reference_code 				:<=			 "referenceCode"
    refund_status 				:<=			 "refundStatus"
    payment_status 				:<=			 "paymentStatus"
    mca_repayment_amount 			:<=			 "mcaRepaymentAmount"
    order_id 		:<=			 "cardPaymentIdorderGuid"
    check_guid 					:<=			 "checkGuid"
    receipt_token 				:<=			 "receiptToken"
    card_holder_first_name 		:<=			 "cardHolderFirstName"
    card_holder_last_name 			:<=			 "cardHolderLastName"
    is_processed_offline 			:<=			 "isProcessedOffline"
    processing_service 			:<=			 "processingService"
    authorized_amount 			:<=			 "authorizedAmount"
    card_tender_type 				:<=			 "cardTenderType"
    prepaid_card_balance 			:<=			 "prepaidCardBalance"
    pro_rated_discount_amount 		:<=			 "proRatedDiscountAmount"
    pro_rated_tax_amount 			:<=			 "proRatedTaxAmount"
    pro_rated_total_service_charge_amount :<=		 "proRatedTotalServiceChargeAmount"
    created_device_id       :<= "createdDevice.id"
    last_modified_device_id       :<= "lastModifiedDevice.id")
  (dynamic-fields
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "server")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "cashDrawer")
    (flatten-fields
      (fields
        amount 			:<=			 "refundAmount"
        tip_amount 			:<=			 "tipRefundAmount"
        date 			:<=			 "refundDate"
        business_date 			:<=			 "refundBusinessDate"
        transaction_guid 			:<=			 "refundTransaction.guid"
        transaction_entity_type 			:<=			 "refundTransaction.entityType"
        strategy 			:<=			 "refundStrategy") :from "refund")
    (flatten-fields
      (fields
        user_guid 			:<=			 "voidUser.guid"
        approver.guid 			:<=			 "voidApprover.guid"
        date 			:<=			 "voidDate"
        business_date 			:<=			 "voidBusinessDate"
        reason_guid 			:<=			 "voidReason.guid"
        reason_entity_type 			:<=			 "voidReason.entityType"
        reason_external_id 			:<=			 "voidReason.externalId") :from "voidInfo")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "houseAccount")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "otherPayment")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType") :from "paymentCardToken")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        stored_value_balance 			:<=			 "storedValueBalance"
        rewards_balance 			:<=			 "rewardsBalance"
        auth_request_id 			:<=			 "authRequestId"
        authorization_state 			:<=			 "authorizationState"
        card_number_identifier 			:<=			 "cardNumberIdentifier") :from "giftCardInfo" :prefix "gift_card_")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "shift")
    (flatten-fields
      (fields
        guid
        entity_type 			:<=			 "entityType"
        external_id 			:<=			 "externalId") :from "serverShift"))
  (relate
    (includes PAYMENT_ID_TEMP :prop "restaurant_id" :as restaurant_id)))
