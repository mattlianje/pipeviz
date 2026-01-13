export const state = {
    currentConfig: null,
    lastRenderedConfigHash: null,
    graphviz: null,
    groupedView: true,
    pipelinesOnlyView: false,
    analysisMode: null, // null, 'critical-path'
    showCostLabels: false,
    expandedGroups: new Set(),
    selectedNode: null,
    // Cached lineage maps for performance
    cachedUpstreamMap: {},
    cachedDownstreamMap: {},
    cachedLineage: {},
    // View state cache for snappy group expand/collapse
    viewStateCache: new Map(),
    // Attribute graph state
    attributeGraphviz: null,
    attributeLastRenderedConfigHash: null,
    nestedClusterCount: 0,
    attributeLineageMap: {},
    datasourceLineageMap: {},
    selectedAttribute: null
}

export function getConfigHash(config) {
    return JSON.stringify(config)
}

// Generate a unique key for the current view state (for caching)
export function getViewStateKey() {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'
    const expandedGroupsKey = Array.from(state.expandedGroups).sort().join(',')
    return `${state.groupedView}|${state.pipelinesOnlyView}|${isDark}|${expandedGroupsKey}`
}

// Clear view state cache (call when config changes)
export function clearViewStateCache() {
    state.viewStateCache.clear()
}

// Max cache entries to prevent memory issues with many group combinations
const MAX_VIEW_CACHE_SIZE = 50

// Add entry to cache with LRU eviction
export function addToViewCache(key, value) {
    if (state.viewStateCache.size >= MAX_VIEW_CACHE_SIZE) {
        // Remove oldest entry (first key in Map)
        const firstKey = state.viewStateCache.keys().next().value
        state.viewStateCache.delete(firstKey)
    }
    state.viewStateCache.set(key, value)
}

export const exampleConfig = {
    clusters: [
        {
            name: 'user-processing',
            description: 'User data processing and enrichment cluster'
        },
        {
            name: 'order-management',
            description: 'Order processing and validation cluster'
        },
        {
            name: 'real-time',
            description: 'Real-time streaming data cluster',
            parent: 'order-management'
        },
        {
            name: 'analytics',
            description: 'Analytics and reporting cluster'
        }
    ],
    pipelines: [
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
    ],
    datasources: [
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
}
