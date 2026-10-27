// The example is a mid-size company's data platform. The original hand-written
// core (users, orders, analytics, with attribute lineage) is kept verbatim; the
// rest is written compactly per domain below and expanded into full entries.

const coreClusters = [
    { name: 'user-processing', description: 'User data processing and enrichment cluster' },
    { name: 'order-management', description: 'Order processing and validation cluster' },
    { name: 'real-time', description: 'Real-time streaming data cluster', parent: 'order-management' },
    { name: 'analytics', description: 'Analytics and reporting cluster' }
]

const corePipelines = [
    {
        name: 'user-enrichment',
        description: 'Enriches user data with behavioral signals and ML features',
        input_sources: ['raw_users', 'user_events'],
        output_sources: ['enriched_users'],
        schedule: 'Every 2 hours',
        duration: 45,
        cost: 18.5,
        tags: ['user-data', 'ml', 'enrichment'],
        cluster: 'user-processing',
        links: {
            airflow: 'https://airflow.company.com/dags/user_enrichment',
            monitoring: 'https://grafana.company.com/d/user-enrichment',
            docs: 'https://docs.company.com/pipelines/user-enrichment'
        },
        upstream_pipelines: []
    },
    {
        name: 'order-processing',
        description: 'Validates and processes incoming orders in real-time',
        input_sources: ['raw_orders', 'inventory'],
        output_sources: ['processed_orders', 'order_audit'],
        schedule: 'Every 15 minutes',
        duration: 12,
        cost: 4.2,
        tags: ['orders', 'real-time', 'validation'],
        cluster: 'real-time',
        links: {
            airflow: 'https://airflow.company.com/dags/order_processing',
            monitoring: 'https://grafana.company.com/d/orders',
            alerts: 'https://pagerduty.company.com/services/orders'
        },
        upstream_pipelines: []
    },
    {
        name: 'analytics-aggregation',
        description: 'Daily aggregation of user metrics and business KPIs',
        input_sources: ['enriched_users', 'processed_orders', 'user_events'],
        output_sources: ['daily_metrics', 'user_cohorts'],
        schedule: 'Daily at 1:00 AM',
        duration: 90,
        cost: 42.0,
        tags: ['analytics', 'aggregation', 'daily'],
        cluster: 'analytics',
        links: {
            airflow: 'https://airflow.company.com/dags/analytics_agg',
            dashboard: 'https://tableau.company.com/analytics-dashboard'
        },
        upstream_pipelines: ['user-enrichment', 'order-processing']
    },
    {
        name: 'export-to-salesforce',
        description: 'Sync user cohorts to Salesforce',
        input_sources: ['user_cohorts'],
        output_sources: ['salesforce_users'],
        group: 'data-exports',
        cluster: 'analytics',
        duration: 8,
        cost: 2.1,
        upstream_pipelines: ['analytics-aggregation'],
        links: {
            airflow: 'https://airflow.company.com/dags/crm_exports'
        }
    },
    {
        name: 'export-to-hubspot',
        description: 'Sync user cohorts to HubSpot',
        input_sources: ['user_cohorts'],
        output_sources: ['hubspot_contacts'],
        group: 'data-exports',
        cluster: 'analytics',
        duration: 5,
        cost: 1.8,
        upstream_pipelines: ['analytics-aggregation'],
        links: {
            airflow: 'https://airflow.company.com/dags/crm_exports'
        }
    },
    {
        name: 'export-to-amplitude',
        description: 'Sync daily metrics to Amplitude',
        input_sources: ['daily_metrics'],
        output_sources: ['amplitude_events'],
        group: 'data-exports',
        cluster: 'analytics',
        duration: 3,
        cost: 0.9,
        upstream_pipelines: ['analytics-aggregation'],
        links: {
            airflow: 'https://airflow.company.com/dags/analytics_exports'
        }
    },
    {
        name: 'weekly-rollup',
        description: 'Aggregate daily metrics into weekly executive summary',
        input_sources: ['daily_metrics', 'enriched_users'],
        output_sources: ['executive_summary'],
        schedule: '0 6 * * MON',
        duration: 120,
        cost: 65.0,
        cluster: 'analytics',
        upstream_pipelines: ['analytics-aggregation', 'user-enrichment'],
        links: {
            airflow: 'https://airflow.company.com/dags/weekly_rollup'
        }
    }
]

const coreDatasources = [
    {
        name: 'raw_users',
        description: 'Raw user registration and profile data from production database',
        type: 'snowflake',
        owner: 'data-platform@company.com',
        tags: ['pii', 'users', 'core-data'],
        cluster: 'user-processing',
        attributes: [
            { name: 'id' },
            { name: 'first_name' },
            { name: 'last_name' },
            { name: 'email' },
            { name: 'signup_date' },
            {
                name: 'address',
                attributes: [
                    { name: 'city' },
                    { name: 'zip' },
                    {
                        name: 'geo',
                        attributes: [{ name: 'lat' }, { name: 'lng' }]
                    }
                ]
            }
        ],
        metadata: {
            schema: 'RAW_DATA',
            table: 'USERS',
            size: '2.1TB',
            record_count: '45M',
            refresh_frequency: 'real-time'
        },
        links: {
            snowflake:
                'https://company.snowflakecomputing.com/console#/data/databases/PROD/schemas/RAW_DATA/table/USERS',
            monitoring: 'https://grafana.company.com/d/raw-users',
            docs: 'https://docs.company.com/schemas/raw_users'
        }
    },
    {
        name: 'user_events',
        description: 'Clickstream and interaction events from all digital touchpoints',
        type: 's3',
        owner: 'analytics-team@company.com',
        tags: ['events', 'clickstream', 'large-dataset'],
        cluster: 'analytics',
        attributes: [
            { name: 'event_id' },
            { name: 'user_id', from: 'raw_users::id' },
            { name: 'event_type' },
            { name: 'page_url' },
            { name: 'timestamp' }
        ],
        metadata: {
            bucket: 'company-events-prod',
            size: '15TB',
            record_count: '2.5B',
            file_format: 'parquet'
        },
        links: {
            s3: 'https://s3.console.aws.amazon.com/s3/buckets/company-events-prod',
            athena: 'https://console.aws.amazon.com/athena/home#query'
        }
    },
    {
        name: 'raw_orders',
        description: 'Real-time order data from e-commerce platform',
        type: 'api',
        owner: 'platform-team@company.com',
        tags: ['orders', 'real-time', 'revenue'],
        cluster: 'real-time',
        metadata: {
            endpoint: 'https://api.company.com/v2/orders',
            rate_limit: '1000 req/min',
            record_count: '120M'
        },
        links: {
            api_docs: 'https://docs.company.com/api/orders',
            monitoring: 'https://grafana.company.com/d/orders-api'
        }
    },
    {
        name: 'inventory',
        description: 'Product inventory levels across all warehouses',
        type: 'snowflake',
        owner: 'supply-chain@company.com',
        tags: ['inventory', 'warehouse', 'operational'],
        cluster: 'order-management',
        metadata: {
            schema: 'INVENTORY',
            size: '150GB',
            refresh_frequency: 'every 15 minutes'
        },
        links: {
            snowflake: 'https://company.snowflakecomputing.com/console#/data/databases/PROD/schemas/INVENTORY',
            tableau: 'https://tableau.company.com/views/inventory-dashboard'
        }
    },
    {
        name: 'enriched_users',
        description: 'Enriched user profiles with behavioral features',
        type: 'delta',
        owner: 'data-platform@company.com',
        tags: ['users', 'enriched', 'ml-ready'],
        cluster: 'user-processing',
        attributes: [
            { name: 'user_id', from: 'raw_users::id' },
            { name: 'full_name', from: ['raw_users::first_name', 'raw_users::last_name'] },
            { name: 'email', from: 'raw_users::email' },
            { name: 'event_count', from: 'user_events::event_id' },
            { name: 'last_active', from: 'user_events::timestamp' },
            { name: 'signup_date', from: 'raw_users::signup_date' },
            {
                name: 'location',
                from: 'raw_users::address',
                attributes: [
                    { name: 'city', from: 'raw_users::address::city' },
                    { name: 'zip', from: 'raw_users::address::zip' },
                    { name: 'coords', from: 'raw_users::address::geo' }
                ]
            }
        ]
    },
    {
        name: 'daily_metrics',
        description: 'Aggregated daily business metrics',
        type: 'snowflake',
        owner: 'analytics-team@company.com',
        tags: ['metrics', 'daily', 'kpi'],
        cluster: 'analytics',
        attributes: [
            { name: 'date' },
            { name: 'active_users', from: 'enriched_users::user_id' },
            { name: 'total_events', from: 'user_events::event_id' }
        ]
    },
    {
        name: 'executive_summary',
        description: 'Weekly executive dashboard metrics',
        type: 'snowflake',
        cluster: 'analytics',
        attributes: [
            { name: 'week' },
            { name: 'weekly_active_users', from: 'daily_metrics::active_users' },
            { name: 'weekly_events', from: 'daily_metrics::total_events' },
            { name: 'user_growth', from: ['daily_metrics::active_users', 'enriched_users::signup_date'] }
        ]
    },
    {
        name: 'user_cohorts',
        description: 'User segmentation and cohort definitions',
        type: 'snowflake',
        cluster: 'analytics'
    },
    {
        name: 'salesforce_users',
        description: 'User data synced to Salesforce CRM',
        type: 'api',
        cluster: 'analytics'
    },
    {
        name: 'hubspot_contacts',
        description: 'Contact data synced to HubSpot',
        type: 'api',
        cluster: 'analytics'
    },
    {
        name: 'amplitude_events',
        description: 'Product analytics events sent to Amplitude',
        type: 'api',
        cluster: 'analytics'
    }
]

// Each domain: clusters it adds, a default owner, and its pipelines as
// [name, description, inputs, outputs, schedule, extra?]. `extra` holds any
// other pipeline fields (group, upstream_pipelines, cluster override, tags).
// prettier-ignore
const domains = [
    {
        clusters: [
            { name: 'ingestion', description: 'Landing data from operational systems and vendors' },
            { name: 'cdc', description: 'Change data capture from production databases', parent: 'ingestion' },
            { name: 'event-streams', description: 'Kafka event streams landed to the lake', parent: 'ingestion' },
            { name: 'saas-connectors', description: 'Scheduled pulls from third-party APIs', parent: 'ingestion' }
        ],
        owner: 'data-platform@company.com',
        tags: ['ingestion'],
        pipelines: [
            ['cdc-users', 'Streams changes from the users database', ['pg_users_db'], ['raw_users', 'raw_user_addresses'], '*/10 * * * *', { cluster: 'cdc' }],
            ['cdc-orders', 'Streams order line items and returns from the orders database', ['pg_orders_db'], ['raw_order_items', 'raw_returns'], '*/10 * * * *', { cluster: 'cdc' }],
            ['cdc-payments', 'Streams payment and refund rows from the payments database', ['pg_payments_db'], ['raw_payments', 'raw_refunds'], '*/10 * * * *', { cluster: 'cdc' }],
            ['cdc-catalog', 'Streams the product catalog, categories and prices', ['pg_catalog_db'], ['raw_products', 'raw_categories', 'raw_prices'], '*/30 * * * *', { cluster: 'cdc' }],
            ['land-web-events', 'Lands web clickstream from Kafka into the lake', ['web_clickstream_topic'], ['raw_web_events'], 'Every 5 minutes', { cluster: 'event-streams' }],
            ['land-mobile-events', 'Lands iOS and Android events from Kafka into the lake', ['mobile_events_topic'], ['raw_mobile_events'], 'Every 5 minutes', { cluster: 'event-streams' }],
            ['land-warehouse-scans', 'Lands barcode scans from warehouse handhelds', ['warehouse_scans_topic'], ['raw_inventory_scans'], 'Every 5 minutes', { cluster: 'event-streams' }],
            ['sync-stripe', 'Pulls charges and disputes from Stripe', ['stripe_api'], ['raw_stripe_charges', 'raw_stripe_disputes'], '0 * * * *', { cluster: 'saas-connectors' }],
            ['sync-adyen', 'Pulls settlement reports from Adyen', ['adyen_api'], ['raw_adyen_settlements'], '0 5 * * *', { cluster: 'saas-connectors' }],
            ['sync-zendesk', 'Pulls tickets and comments from Zendesk', ['zendesk_api'], ['raw_tickets', 'raw_ticket_comments'], '0 * * * *', { cluster: 'saas-connectors' }],
            ['sync-google-ads', 'Pulls daily spend from Google Ads', ['google_ads_api'], ['raw_google_ads_spend'], '0 3 * * *', { cluster: 'saas-connectors', group: 'ad-connectors' }],
            ['sync-meta-ads', 'Pulls daily spend from Meta Ads', ['meta_ads_api'], ['raw_meta_ads_spend'], '0 3 * * *', { cluster: 'saas-connectors', group: 'ad-connectors' }],
            ['sync-tiktok-ads', 'Pulls daily spend from TikTok Ads', ['tiktok_ads_api'], ['raw_tiktok_ads_spend'], '0 3 * * *', { cluster: 'saas-connectors', group: 'ad-connectors' }],
            ['sync-braze', 'Pulls email and push engagement events from Braze', ['braze_api'], ['raw_messaging_events'], '0 */2 * * *', { cluster: 'saas-connectors' }],
            ['sync-fx-rates', 'Pulls daily currency exchange rates', ['fx_rates_api'], ['fx_rates'], '0 0 * * *', { cluster: 'saas-connectors' }],
            ['sync-carriers', 'Pulls shipment scans from carrier APIs', ['shipping_carrier_api'], ['raw_shipments'], '*/30 * * * *', { cluster: 'saas-connectors' }],
            ['sync-app-stores', 'Pulls installs and reviews from the App Store and Google Play', ['app_store_api'], ['raw_app_installs', 'raw_app_reviews'], '0 4 * * *', { cluster: 'saas-connectors' }]
        ]
    },
    {
        clusters: [{ name: 'product-analytics', description: 'Sessions, funnels and experiments' }],
        owner: 'analytics-team@company.com',
        tags: ['product', 'events'],
        pipelines: [
            ['bot-filtering', 'Flags crawler and bot traffic in the web clickstream', ['raw_web_events'], ['bot_traffic_flags'], 'Hourly'],
            ['unify-events', 'Merges web and mobile events into one event model, minus bots', ['raw_web_events', 'raw_mobile_events', 'bot_traffic_flags'], ['user_events'], 'Hourly'],
            ['sessionize', 'Groups events into sessions with 30-minute inactivity windows', ['user_events'], ['sessions'], 'Hourly'],
            ['identity-stitching', 'Links anonymous devices to known users', ['user_events', 'raw_users'], ['identity_graph'], '0 2 * * *'],
            ['funnel-builder', 'Builds browse-to-checkout funnel steps', ['sessions', 'user_events'], ['funnel_steps'], '0 3 * * *'],
            ['feature-adoption', 'Tracks first use and repeat use of product features', ['user_events', 'product_catalog'], ['feature_adoption_daily'], '0 3 * * *'],
            ['experiment-exposures', 'Extracts A/B test assignments from the clickstream', ['raw_web_events', 'raw_mobile_events'], ['experiment_exposures'], 'Hourly'],
            ['experiment-analysis', 'Computes lift and significance for running experiments', ['experiment_exposures', 'processed_orders', 'sessions'], ['experiment_results'], '0 6 * * *'],
            ['retention-curves', 'Weekly retention by signup cohort', ['user_cohorts', 'sessions'], ['retention_curves'], '0 7 * * MON'],
            ['mobile-attribution', 'Attributes app installs to campaigns', ['raw_app_installs', 'attribution_touches'], ['mobile_install_attribution'], '0 5 * * *']
        ]
    },
    {
        clusters: [],
        owner: 'data-platform@company.com',
        tags: ['users'],
        cluster: 'user-processing',
        pipelines: [
            ['address-normalization', 'Standardises and geocodes user addresses', ['raw_user_addresses'], ['normalized_addresses'], '0 1 * * *'],
            ['user-master', 'Deduplicates users across devices and accounts', ['raw_users', 'identity_graph'], ['user_master'], '0 3 * * *'],
            ['user-preferences', 'Derives category and channel preferences', ['user_master', 'user_events'], ['user_preferences'], '0 4 * * *'],
            ['churn-labels', 'Labels users with no activity or orders in 60 days', ['user_master', 'sessions', 'processed_orders'], ['churn_labels'], '0 4 * * *'],
            ['gdpr-erasure', 'Applies right-to-be-forgotten requests across user tables', ['user_master', 'erasure_requests'], ['erasure_log'], '0 0 * * *', { tags: ['users', 'compliance', 'pii'] }]
        ]
    },
    {
        clusters: [{ name: 'catalog', description: 'Products, prices and stock' }],
        owner: 'merchandising-data@company.com',
        tags: ['catalog'],
        pipelines: [
            ['build-product-catalog', 'Joins products, categories and current prices', ['raw_products', 'raw_categories', 'raw_prices'], ['product_catalog'], 'Hourly'],
            ['price-history', 'Keeps a slowly-changing history of every price change', ['raw_prices'], ['price_history'], '0 1 * * *'],
            ['inventory-snapshot', 'Rolls warehouse scans up into stock levels', ['raw_inventory_scans'], ['inventory'], 'Every 15 minutes', { cluster: 'order-management', tags: ['inventory', 'operational'] }],
            ['stockout-detection', 'Flags products about to run out of stock', ['inventory', 'product_catalog', 'demand_forecast'], ['stockout_alerts'], 'Hourly'],
            ['product-popularity', 'Views, add-to-carts and purchases per product', ['user_events', 'processed_orders'], ['product_popularity'], '0 2 * * *'],
            ['search-index-docs', 'Builds documents for the product search index', ['product_catalog', 'product_popularity'], ['search_index_docs'], '0 */4 * * *']
        ]
    },
    {
        clusters: [],
        owner: 'platform-team@company.com',
        tags: ['orders'],
        cluster: 'order-management',
        pipelines: [
            ['flatten-order-lines', 'One row per order line with product attributes', ['raw_order_items', 'product_catalog'], ['order_lines'], 'Hourly'],
            ['returns-processing', 'Matches returns to their original order lines', ['raw_returns', 'order_lines'], ['returns'], 'Hourly'],
            ['fraud-scoring', 'Scores every order for fraud risk before fulfilment', ['processed_orders', 'payment_events', 'fs_user_risk'], ['fraud_scores'], 'Every 5 minutes', { cluster: 'real-time', tags: ['orders', 'real-time', 'risk'] }],
            ['shipment-tracking', 'Joins carrier scans to orders', ['raw_shipments', 'processed_orders'], ['shipment_status'], '*/30 * * * *'],
            ['delivery-sla', 'Daily on-time delivery rate per carrier and region', ['shipment_status', 'normalized_addresses'], ['delivery_sla_daily'], '0 6 * * *'],
            ['order-facts', 'The order fact table everything downstream reports from', ['processed_orders', 'order_lines', 'returns', 'payments_ledger'], ['fct_orders'], '0 2 * * *']
        ]
    },
    {
        clusters: [
            { name: 'finance', description: 'Revenue, close and reporting' },
            { name: 'payments', description: 'Payment events and the ledger', parent: 'finance' }
        ],
        owner: 'finance-data@company.com',
        tags: ['finance'],
        pipelines: [
            ['normalize-payments', 'One payment event model across the processor and PSPs', ['raw_payments', 'raw_stripe_charges', 'raw_adyen_settlements'], ['payment_events'], 'Every 15 minutes', { cluster: 'payments' }],
            ['normalize-refunds', 'One refund model across refunds and disputes', ['raw_refunds', 'raw_stripe_disputes'], ['refund_events'], 'Hourly', { cluster: 'payments' }],
            ['payments-ledger', 'Double-entry ledger of every money movement in USD', ['payment_events', 'refund_events', 'fx_rates'], ['payments_ledger'], '0 1 * * *', { cluster: 'payments' }],
            ['chargeback-monitor', 'Links disputes to fraud scores for review', ['raw_stripe_disputes', 'fraud_scores'], ['chargeback_cases'], 'Hourly', { cluster: 'payments' }],
            ['payments-reconciliation', 'Reconciles the ledger against PSP settlement reports', ['payments_ledger', 'raw_adyen_settlements'], ['reconciliation_breaks'], '0 6 * * *', { cluster: 'payments' }],
            ['revenue-recognition', 'Recognises revenue on delivery, net of returns', ['fct_orders', 'payments_ledger', 'shipment_status'], ['revenue_recognized'], '0 3 * * *'],
            ['daily-revenue', 'Daily revenue by country, channel and currency', ['revenue_recognized', 'fx_rates'], ['daily_revenue'], '0 4 * * *'],
            ['gross-margin', 'Margin per product after cost of goods and returns', ['daily_revenue', 'order_lines', 'price_history'], ['gross_margin'], '0 5 * * *'],
            ['tax-reporting', 'Sales tax and VAT liability by jurisdiction', ['revenue_recognized', 'normalized_addresses'], ['tax_liability'], '0 6 1 * *'],
            // Runs after reconciliation without reading what it writes: the books
            // aren't closed until breaks are cleared by hand.
            ['month-end-close', 'Month-end close package for the accounting team', ['revenue_recognized', 'payments_ledger', 'ad_spend_daily'], ['month_end_close'], '0 8 2 * *', { upstream_pipelines: ['payments-reconciliation'] }],
            ['export-to-netsuite', 'Posts month-end journal entries to NetSuite', ['month_end_close'], ['netsuite_journal'], '0 10 2 * *'],
            ['board-pack', 'Monthly board metrics pack', ['month_end_close', 'executive_summary', 'customer_ltv', 'cac_by_channel'], ['board_pack'], '0 12 3 * *']
        ]
    },
    {
        clusters: [{ name: 'marketing', description: 'Spend, attribution and audiences' }],
        owner: 'marketing-analytics@company.com',
        tags: ['marketing'],
        pipelines: [
            ['unify-ad-spend', 'Daily spend per campaign across ad platforms, in USD', ['raw_google_ads_spend', 'raw_meta_ads_spend', 'raw_tiktok_ads_spend', 'fx_rates'], ['ad_spend_daily'], '0 5 * * *'],
            ['attribution-touches', 'Extracts UTM-tagged marketing touches per user', ['user_events', 'identity_graph'], ['attribution_touches'], '0 4 * * *'],
            ['multi-touch-attribution', 'Splits order revenue across touches with a position-based model', ['attribution_touches', 'fct_orders'], ['order_attribution'], '0 5 * * *'],
            ['campaign-roi', 'Return on ad spend per campaign', ['order_attribution', 'ad_spend_daily'], ['campaign_roi'], '0 6 * * *'],
            ['cac-by-channel', 'Customer acquisition cost per channel and cohort', ['campaign_roi', 'user_master', 'mobile_install_attribution'], ['cac_by_channel'], '0 7 * * *'],
            ['messaging-engagement', 'Open, click and unsubscribe rates per send', ['raw_messaging_events', 'user_master'], ['messaging_engagement'], '0 4 * * *'],
            ['audience-builder', 'Builds marketing audiences from cohorts and model scores', ['user_cohorts', 'churn_predictions', 'user_preferences', 'messaging_engagement'], ['marketing_audiences'], '0 8 * * *'],
            ['push-audiences-google', 'Uploads audiences to Google Customer Match', ['marketing_audiences'], ['google_customer_match'], '0 9 * * *', { group: 'audience-exports' }],
            ['push-audiences-meta', 'Uploads audiences to Meta Custom Audiences', ['marketing_audiences'], ['meta_custom_audiences'], '0 9 * * *', { group: 'audience-exports' }],
            ['push-audiences-braze', 'Syncs audiences to Braze segments', ['marketing_audiences'], ['braze_segments'], '0 9 * * *', { group: 'audience-exports' }]
        ]
    },
    {
        clusters: [
            { name: 'ml-platform', description: 'Model training and batch inference' },
            { name: 'feature-store', description: 'Offline features shared across models', parent: 'ml-platform' }
        ],
        owner: 'ml-platform@company.com',
        tags: ['ml'],
        pipelines: [
            ['fs-user-features', 'Per-user behavioural and purchase features', ['enriched_users', 'sessions', 'fct_orders'], ['fs_user_features'], '0 4 * * *', { cluster: 'feature-store' }],
            ['fs-user-risk', 'Per-user payment risk features', ['payment_events', 'identity_graph'], ['fs_user_risk'], 'Hourly', { cluster: 'feature-store' }],
            ['fs-product-features', 'Per-product popularity and price features', ['product_popularity', 'product_catalog', 'price_history'], ['fs_product_features'], '0 4 * * *', { cluster: 'feature-store' }],
            ['train-churn-model', 'Retrains the churn classifier', ['fs_user_features', 'churn_labels'], ['churn_model'], '0 2 * * SUN'],
            ['score-churn', 'Daily churn probability for every active user', ['churn_model', 'fs_user_features'], ['churn_predictions'], '0 6 * * *'],
            ['train-recs-model', 'Retrains the product recommendation model', ['fs_product_features', 'fs_user_features', 'user_events'], ['recs_model'], '0 3 * * SUN'],
            ['score-recs', 'Top-50 product recommendations per user', ['recs_model', 'fs_user_features', 'stockout_alerts'], ['user_recommendations'], '0 6 * * *'],
            ['demand-forecast', 'Two-week demand forecast per product and warehouse', ['order_lines', 'inventory', 'price_history'], ['demand_forecast'], '0 2 * * *'],
            ['customer-ltv', 'Predicted 12-month lifetime value per customer', ['fct_orders', 'fs_user_features', 'returns'], ['customer_ltv'], '0 7 * * *'],
            ['model-monitoring', 'Feature drift and accuracy against realised labels', ['churn_predictions', 'churn_labels', 'user_recommendations', 'fct_orders'], ['model_drift_metrics'], '0 9 * * *']
        ]
    },
    {
        clusters: [{ name: 'support', description: 'Customer support and voice of customer' }],
        owner: 'cx-analytics@company.com',
        tags: ['support'],
        pipelines: [
            ['normalize-tickets', 'One row per ticket with first-response and resolution times', ['raw_tickets', 'raw_ticket_comments', 'user_master'], ['support_tickets'], 'Hourly'],
            ['extract-erasure-requests', 'Picks out GDPR deletion requests from tickets', ['support_tickets'], ['erasure_requests'], 'Hourly', { tags: ['support', 'compliance'] }],
            ['ticket-sentiment', 'Scores ticket sentiment and topic', ['support_tickets'], ['ticket_sentiment'], '0 */2 * * *'],
            ['csat-daily', 'Daily satisfaction by queue and topic', ['support_tickets', 'ticket_sentiment'], ['csat_daily'], '0 5 * * *'],
            ['app-review-sentiment', 'Topics and sentiment from app store reviews', ['raw_app_reviews'], ['app_review_sentiment'], '0 6 * * *'],
            ['support-cost-per-order', 'Support contacts and cost per order', ['support_tickets', 'fct_orders'], ['support_cost_per_order'], '0 6 * * *']
        ]
    },
    {
        clusters: [],
        owner: 'analytics-team@company.com',
        tags: ['analytics', 'reporting'],
        cluster: 'analytics',
        pipelines: [
            ['kpi-dashboard', 'Company KPI dashboard tables', ['daily_metrics', 'daily_revenue', 'csat_daily', 'delivery_sla_daily'], ['kpi_dashboard'], '0 7 * * *'],
            ['product-health-report', 'Weekly product health report for product managers', ['funnel_steps', 'feature_adoption_daily', 'retention_curves', 'app_review_sentiment'], ['product_health_report'], '0 8 * * MON'],
            ['experiment-report', 'Weekly digest of experiment results', ['experiment_results', 'gross_margin'], ['experiment_digest'], '0 9 * * MON']
        ]
    },
    {
        clusters: [{ name: 'data-quality', description: 'Freshness, volume and schema checks' }],
        owner: 'data-platform@company.com',
        tags: ['data-quality'],
        pipelines: [
            ['dq-freshness', 'Checks that critical tables landed on time', ['raw_users', 'raw_orders', 'raw_payments', 'user_events'], ['dq_freshness_results'], 'Every 15 minutes', { group: 'dq-checks' }],
            ['dq-volume', 'Flags day-over-day row count anomalies', ['fct_orders', 'payments_ledger', 'sessions'], ['dq_volume_results'], 'Hourly', { group: 'dq-checks' }],
            ['dq-schema-drift', 'Detects new, dropped and retyped event fields', ['raw_web_events', 'raw_mobile_events'], ['dq_schema_results'], 'Hourly', { group: 'dq-checks' }],
            ['dq-alerting', 'Opens incidents for failed checks and pages the owner', ['dq_freshness_results', 'dq_volume_results', 'dq_schema_results'], ['dq_incidents'], 'Every 15 minutes'],
            ['dq-scorecard', 'Weekly data quality scorecard per owning team', ['dq_incidents'], ['dq_scorecard'], '0 8 * * MON']
        ]
    },
    {
        clusters: [{ name: 'reverse-etl', description: 'Warehouse data pushed back into operational tools' }],
        owner: 'data-platform@company.com',
        tags: ['reverse-etl'],
        pipelines: [
            ['push-ltv-to-salesforce', 'Writes predicted LTV onto Salesforce accounts', ['customer_ltv'], ['salesforce_account_ltv'], '0 8 * * *', { group: 'crm-sync' }],
            ['push-churn-to-zendesk', 'Tags at-risk users in Zendesk for priority support', ['churn_predictions'], ['zendesk_user_flags'], '0 8 * * *', { group: 'crm-sync' }],
            ['push-recs-to-app', 'Loads recommendations into the app cache', ['user_recommendations'], ['app_recs_cache'], '0 7 * * *'],
            ['push-search-index', 'Reindexes products in Elasticsearch', ['search_index_docs'], ['elasticsearch_products'], '0 */4 * * *']
        ]
    }
]

// Descriptions for the sources that land from outside; the rest are described
// by the pipeline that writes them.
const externalSources = {
    pg_users_db: 'Production Postgres for accounts and profiles',
    pg_orders_db: 'Production Postgres for orders and returns',
    pg_payments_db: 'Production Postgres for the payment service',
    pg_catalog_db: 'Production Postgres for the product catalog',
    web_clickstream_topic: 'Kafka topic of web tracking events',
    mobile_events_topic: 'Kafka topic of mobile app events',
    warehouse_scans_topic: 'Kafka topic of warehouse barcode scans',
    stripe_api: 'Stripe REST API',
    adyen_api: 'Adyen settlement report API',
    zendesk_api: 'Zendesk Support API',
    google_ads_api: 'Google Ads reporting API',
    meta_ads_api: 'Meta Marketing API',
    tiktok_ads_api: 'TikTok Ads reporting API',
    braze_api: 'Braze Currents export',
    fx_rates_api: 'Daily reference exchange rates',
    shipping_carrier_api: 'Carrier tracking webhooks and APIs',
    app_store_api: 'App Store Connect and Google Play Console'
}

function sourceType(name) {
    if (name.startsWith('pg_')) return 'postgres'
    if (name.endsWith('_topic')) return 'kafka'
    if (name.endsWith('_api') || name.endsWith('_cache')) return 'api'
    if (/^raw_(web|mobile)_events$|^raw_inventory_scans$/.test(name)) return 's3'
    if (name.startsWith('fs_') || name.endsWith('_model')) return 'delta'
    return 'snowflake'
}

// Stable pseudo-random numbers per pipeline, so the stats tab has something to show.
function hash(s) {
    let h = 2166136261
    for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619)
    return (h >>> 0) / 2 ** 32
}

function expand() {
    const clusters = [...coreClusters]
    const pipelines = [...corePipelines]
    const datasources = new Map(coreDatasources.map((d) => [d.name, d]))
    const writers = new Map()
    const readers = new Map()
    const register = (p) => {
        p.output_sources.forEach((s) => writers.has(s) || writers.set(s, p))
        p.input_sources.forEach((s) => readers.has(s) || readers.set(s, p))
    }
    corePipelines.forEach(register)

    domains.forEach((domain) => {
        clusters.push(...domain.clusters)
        const homeCluster = domain.cluster || domain.clusters[0]?.name
        domain.pipelines.forEach(([name, description, inputs, outputs, schedule, extra = {}]) => {
            const r = hash(name)
            const p = {
                name,
                description,
                input_sources: inputs,
                output_sources: outputs,
                schedule,
                duration: Math.round(3 + r * r * 110),
                cost: Math.round((0.5 + r * r * 40) * 10) / 10,
                tags: domain.tags,
                cluster: homeCluster,
                owner: domain.owner,
                links: { airflow: `https://airflow.company.com/dags/${name.replace(/-/g, '_')}` },
                ...extra
            }
            pipelines.push(p)
            register(p)
        })
    })

    // Every source referenced but not defined above gets an entry, homed in the
    // cluster of whatever writes it (or, for external sources, first reads it).
    ;[...writers.keys(), ...readers.keys()].forEach((name) => {
        if (datasources.has(name)) return
        const writer = writers.get(name)
        const owner = writer || readers.get(name)
        const ds = {
            name,
            type: sourceType(name),
            owner: (writer && owner.owner) || 'platform-team@company.com',
            cluster: owner.cluster
        }
        if (!writer) {
            ds.description = externalSources[name]
            ds.tags = ['external']
        }
        datasources.set(name, ds)
    })

    return { clusters, pipelines, datasources: [...datasources.values()] }
}

export const exampleConfig = expand()
