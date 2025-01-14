relationships = [
    {
        "parent_table": "orders_check",
        "child_fk_field": "orders_check_id",
        "child_fk_value": "p[\"guid\"]",
        "child_table": [
            {
                "field": "payments",
                "child_table": "orders_check_payment"
            },
            {
                "field": "selections",
                "child_table": "orders_check_selection"
            },
            {
                "field": "appliedDiscounts",
                "child_table": "orders_check_applied_discount"
            }
        ]
    },
    {
        "parent_table": "orders_check_applied_discount",
        "child_fk_field": "orders_check_applied_discount_id",
        "child_fk_value": "p[\"guid\"]",
        "child_table": [
            {
                "field": "comboItems",
                "child_table": "orders_check_applied_discount_combo_item"
            },
            {
                "field": "triggers",
                "child_table": "orders_check_applied_discount_trigger"
            }
        ]
    }
]