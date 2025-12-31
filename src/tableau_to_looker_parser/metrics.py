"""Generate Tableau usage and user metrics directly from PostgreSQL.

This script:

1) Receives Tableau site LUID and Postgres connection info from command line arguments.

2) Runs a set of PostgreSQL queries filtered by that site id.

3) Writes a JSON file named metrics_<site-id>.json containing all results.

"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

import psycopg
from psycopg.rows import dict_row


@dataclass
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str
    connect_timeout: int


def pg_connect(cfg: DbConfig) -> psycopg.Connection:
    """Create a PostgreSQL connection using psycopg."""
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        sslmode=cfg.sslmode,
        connect_timeout=cfg.connect_timeout,
    )


def run_query(sql: str, site_id: str, conn: psycopg.Connection) -> List[Dict[str, Any]]:
    """Run a parameterized query with site_id and return list of dictionaries."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"site_id": site_id})
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def rows_to_indexed_map(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Convert a list of row dicts to a row-keyed mapping for JSON output."""
    return {f"row{idx+1}": row for idx, row in enumerate(rows)}


def total_workbooks_query() -> str:
    """Query to get total workbook count for a site."""
    return """
    SELECT COUNT(DISTINCT w.id) AS total_workbooks
    FROM workbooks w
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s;
    """


def inactive_workbooks_count_query() -> str:
    """Query to get total count of inactive workbooks (for metrics calculation)."""
    return """
WITH recent_workbook_views AS (
    SELECT DISTINCT w.id AS workbook_id
    FROM workbooks w
    INNER JOIN views v ON v.workbook_id = w.id
    INNER JOIN hist_views hv ON hv.view_id = v.id
    INNER JOIN historical_events he ON he.hist_view_id = hv.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
)
SELECT COUNT(DISTINCT w.id) AS inactive_workbooks_count
FROM workbooks w
INNER JOIN sites s ON w.site_id = s.id
WHERE s.luid = %(site_id)s
  AND w.id NOT IN (SELECT workbook_id FROM recent_workbook_views);
"""


def calculate_inactive_workbooks_metrics(
    inactive_workbooks_count: int, total_workbooks: int
) -> Dict[str, Any]:
    """Calculate metrics for inactive workbooks section.
    
    Args:
        inactive_workbooks_count: Total count of inactive workbooks (from count query)
        total_workbooks: Total number of workbooks in the site
        
    Returns:
        Dict with inactive_count, total_count, and percentage
    """
    percentage = (inactive_workbooks_count / total_workbooks * 100) if total_workbooks > 0 else 0
    
    return {
        "inactive_count": inactive_workbooks_count,
        "total_count": total_workbooks,
        "percentage": round(percentage, 1)
    }


def calculate_content_creation_rate_metrics(
    content_creation_rate: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Calculate metrics for content creation rate section.
    
    Args:
        content_creation_rate: List of monthly creation rate records
        
    Returns:
        Dict with activity_level (e.g., "highly active", "moderately active", "low activity")
    """
    if not content_creation_rate:
        return {"activity_level": "unknown"}
    
    # Calculate average workbooks created and modified per month
    total_created = sum(
        int(row.get("new_workbooks_created", 0)) for row in content_creation_rate
    )
    total_modified = sum(
        int(row.get("workbooks_modified", 0)) for row in content_creation_rate
    )
    total_activity = total_created + total_modified
    months_with_data = len([r for r in content_creation_rate if int(r.get("new_workbooks_created", 0)) > 0 or int(r.get("workbooks_modified", 0)) > 0])
    avg_per_month = total_activity / len(content_creation_rate) if content_creation_rate else 0
    
    # Determine activity level
    if avg_per_month >= 5:
        activity_level = "highly active"
    elif avg_per_month >= 2:
        activity_level = "moderately active"
    else:
        activity_level = "low activity"
    
    return {
        "activity_level": activity_level,
        "avg_per_month": round(avg_per_month, 1),
        "total_created": total_created,
        "months_with_data": months_with_data
    }


def calculate_developer_activity_metrics(
    developer_activity: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Calculate metrics for developer activity section.
    
    Args:
        developer_activity: List of developer activity records (should have number_of_active_developers)
        
    Returns:
        Dict with developer_count and time_period_days
    """
    if not developer_activity:
        return {"developer_count": 0, "time_period_days": 90}
    
    # Extract from first row (aggregated result)
    first_row = developer_activity[0]
    dev_count = int(first_row.get("number_of_active_developers", 0))
    
    # Time period is 90 days based on the query (90 days for examples, 300 for main)
    return {
        "developer_count": dev_count,
        "time_period_days": 90
    }


def generate_table_descriptions(calculated_metrics: Dict[str, Any], results: Dict[str, List[Dict[str, Any]]] = None) -> Dict[str, str]:
    """Generate description text for each table based on calculated metrics.
    
    Args:
        calculated_metrics: Dict containing all calculated metrics
        results: Dict containing query results (optional, for dynamic counts)
        
    Returns:
        Dict mapping table names to their descriptions
    """
    inactive_metrics = calculated_metrics.get("inactive_workbooks_summary", {})
    content_metrics = calculated_metrics.get("content_creation_rate", {})
    developer_metrics = calculated_metrics.get("developer_activity", {})
    
    # Calculate report count for Recommended Pilot Reports
    # Count reports (only high/medium usage reports are in the table)
    # The query filters to only include reports with >= 5 views (Moderate or High Usage)
    # High Usage: >= 10 views
    # Moderate Usage: >= 5 views
    # Load time can be anything (fast, moderate, or slow)
    report_count = 0
    if results and "recommended_pilot_reports" in results:
        report_count = len(results["recommended_pilot_reports"])
    
    return {
        "Most_Used_Workbooks_Summary": (
            "The following metrics detail which workbooks are most valuable to the organization "
            "and which can be decommissioned to reduce migration scope."
        ),
        "Most_Used_Workbooks_Detailed": (
            "Detailed view-level breakdown of most used workbooks showing individual content items "
            "(dashboards, views, and stories) that are actively being accessed. This granular view "
            "helps identify specific high-value content within popular workbooks."
        ),
        "Inactive_Workbooks_Summary": (
            f"A significant portion of the content is stale and can be decommissioned. "
            f"Over \"{inactive_metrics.get('percentage', 0)}%\" of all workbooks "
            f"\"({inactive_metrics.get('total_count', 0)} total)\" have not been viewed "
            f"in the last 60 days. This represents a major opportunity to reduce the migration scope."
        ),
        "Inactive_Workbooks_Detailed": (
            "Detailed view-level breakdown of inactive workbooks showing individual content items "
            "(dashboards and views and stories) that have not been accessed in the last 60 days. This granular view "
            "helps identify specific content that can be decommissioned."
        ),
        "Content_Creation_Rate": (
            f"The environment is \"{content_metrics.get('activity_level', 'unknown')}\". "
            f"The rate of content creation indicates a strong demand for self-service that a governed platform "
            f"like Looker can properly support."
        ),
        "Top_Users_Activity": (
            "Understanding user roles and behaviors is key to a successful adoption strategy."
        ),
        "Developer_Activity": (
            f"Content creation is highly distributed. An estimated \"~{developer_metrics.get('developer_count', 0)}\" "
            f"unique users have published at least one workbook in the last \"{developer_metrics.get('time_period_days', 90)}\" days. "
            f"This broad, decentralized developer base confirms a strong desire for self-service analytics "
            f"that is currently happening in an ungoverned manner."
        ),
        "Frequently_Used_Slow_Reports_Summary": (
            "Workbook-level summary of frequently used reports with slow performance. These workbooks are "
            "heavily accessed but suffer from slow load times, creating a poor user experience and representing "
            "a key pain point to be solved by the migration."
        ),
        "Frequently_Used_Slow_Reports_Detailed": (
            "Detailed view-level breakdown of frequently used slow reports showing individual content items "
            "(dashboards, views, and stories) that are heavily accessed but have slow load times. This granular view "
            "helps identify specific high-impact performance issues within popular workbooks."
        ),
        "Recommended_Pilot_Reports": (
            f'The {report_count} reports below are recommended for the pilot migration. '
            f'They represent a mix of high-impact slow reports that need optimization and efficient, '
            f'critical reports that are central to business operations. Migrating them will not only '
            f'solve key performance pain points but also establish a trusted semantic layer for future '
            f'conversational analytics and self-service exploration.'
        ),
    }

def most_used_workbooks_summary_query() -> str:
    """Query for most used workbooks summary (workbook level aggregation)."""
    return """
WITH workbook_usage AS (
    SELECT
        w.id AS workbook_id,
        w.name AS workbook_name,
        he.hist_actor_user_id,
        COUNT(he.id) AS user_view_count
    FROM workbooks w
    INNER JOIN views v ON v.workbook_id = w.id
    INNER JOIN hist_views hv ON hv.view_id = v.id
    INNER JOIN historical_events he ON he.hist_view_id = hv.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name, he.hist_actor_user_id
),
max_views_per_workbook AS (
    SELECT 
        workbook_id,
        MAX(user_view_count) AS max_view_count
    FROM workbook_usage
    GROUP BY workbook_id
),
top_users_per_workbook AS (
    SELECT 
        wu.workbook_id,
        wu.workbook_name,
        STRING_AGG(hu.name, ', ' ORDER BY hu.name) AS primary_user_names
    FROM workbook_usage wu
    INNER JOIN max_views_per_workbook mv ON wu.workbook_id = mv.workbook_id 
        AND wu.user_view_count = mv.max_view_count
    INNER JOIN hist_users hu ON wu.hist_actor_user_id = hu.id
    GROUP BY wu.workbook_id, wu.workbook_name
),
workbook_totals AS (
    SELECT
        w.id AS workbook_id,
        w.name AS workbook_name,
        COUNT(he.id) AS total_views
    FROM workbooks w
    INNER JOIN hist_workbooks hw ON hw.workbook_id = w.id
    INNER JOIN historical_events he ON he.hist_workbook_id = hw.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name
),
workbook_data AS (
    SELECT
        wt.workbook_id,
        wt.workbook_name,
        wt.total_views AS views_last_60_days,
        tup.primary_user_names AS primary_user_name
    FROM workbook_totals wt
    INNER JOIN top_users_per_workbook tup ON wt.workbook_id = tup.workbook_id
),
ranked_workbooks AS (
    SELECT
        workbook_name,
        views_last_60_days,
        primary_user_name,
        ROW_NUMBER() OVER (ORDER BY views_last_60_days DESC) AS rank
    FROM workbook_data
    ORDER BY views_last_60_days DESC
    LIMIT 50
)
SELECT
    rank,
    workbook_name,
    views_last_60_days,
    primary_user_name
FROM ranked_workbooks
ORDER BY rank;
"""


def most_used_workbooks_detailed_query() -> str:
    """Query for most used workbooks detailed view (view level granularity)."""
    return """
WITH view_usage AS (
    SELECT
        w.id AS workbook_id,
        w.name AS workbook_name,
        v.id AS view_id,
        v.name AS view_name,
        v.sheettype AS content_type,
        he.hist_actor_user_id,
        COUNT(he.id) AS user_view_count
    FROM workbooks w
    INNER JOIN views v ON v.workbook_id = w.id
    INNER JOIN hist_views hv ON hv.view_id = v.id
    INNER JOIN historical_events he ON he.hist_view_id = hv.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name, v.id, v.name, v.sheettype, he.hist_actor_user_id
),
max_views_per_view AS (
    SELECT 
        view_id,
        MAX(user_view_count) AS max_view_count
    FROM view_usage
    GROUP BY view_id
),
top_users_per_view AS (
    SELECT 
        vu.view_id,
        vu.workbook_id,
        vu.workbook_name,
        vu.view_name,
        vu.content_type,
        STRING_AGG(hu.name, ', ' ORDER BY hu.name) AS primary_user_names
    FROM view_usage vu
    INNER JOIN max_views_per_view mv ON vu.view_id = mv.view_id 
        AND vu.user_view_count = mv.max_view_count
    INNER JOIN hist_users hu ON vu.hist_actor_user_id = hu.id
    GROUP BY vu.view_id, vu.workbook_id, vu.workbook_name, vu.view_name, vu.content_type
),
view_totals AS (
    SELECT
        w.id AS workbook_id,
        w.name AS workbook_name,
        v.id AS view_id,
        v.name AS view_name,
        COALESCE(v.sheettype, 'Unknown') AS content_type,
        COUNT(he.id) AS total_views
    FROM workbooks w
    INNER JOIN views v ON v.workbook_id = w.id
    INNER JOIN hist_views hv ON hv.view_id = v.id
    INNER JOIN historical_events he ON he.hist_view_id = hv.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name, v.id, v.name, v.sheettype
),
content_data AS (
    SELECT
        vt.workbook_name,
        vt.view_name AS content_name,
        vt.content_type,
        vt.total_views AS views_last_60_days,
        tuv.primary_user_names AS primary_user_name
    FROM view_totals vt
    INNER JOIN top_users_per_view tuv ON vt.view_id = tuv.view_id
),
ranked_content AS (
    SELECT
        workbook_name,
        content_name,
        content_type,
        views_last_60_days,
        primary_user_name,
        ROW_NUMBER() OVER (ORDER BY views_last_60_days DESC, workbook_name, content_type, content_name) AS rank
    FROM content_data
    ORDER BY views_last_60_days DESC, workbook_name, content_type, content_name
    LIMIT 50
)
SELECT
    rank,
    workbook_name,
    content_name,
    content_type,
    views_last_60_days,
    primary_user_name
FROM ranked_content
ORDER BY rank;
"""


def inactive_workbooks_detailed_query() -> str:
    """Query for inactive workbooks detailed view (view level granularity)."""
    return """
WITH recent_workbook_views AS (
    SELECT DISTINCT w.id AS workbook_id
    FROM workbooks w
    INNER JOIN views v ON v.workbook_id = w.id
    INNER JOIN hist_views hv ON hv.view_id = v.id
    INNER JOIN historical_events he ON he.hist_view_id = hv.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
),
last_view_access_per_content AS (
    SELECT
        v.id AS view_id,
        v.workbook_id,
        v.name AS view_name,
        MAX(he.created_at) AS last_viewed_at
    FROM views v
    INNER JOIN workbooks w ON v.workbook_id = w.id
    INNER JOIN sites s ON w.site_id = s.id
    LEFT JOIN hist_views hv ON hv.view_id = v.id
    LEFT JOIN historical_events he ON he.hist_view_id = hv.id
    LEFT JOIN historical_event_types het
        ON he.historical_event_type_id = het.type_id
       AND het.name IN ('Access View', 'Access Authoring View')
    WHERE s.luid = %(site_id)s
    GROUP BY v.id, v.workbook_id, v.name
),
inactive_workbooks AS (
    SELECT DISTINCT w.id AS workbook_id
    FROM workbooks w
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND w.id NOT IN (SELECT workbook_id FROM recent_workbook_views)
),
content_data AS (
    SELECT
        w.name AS workbook_name,
        v.name AS content_name,
        COALESCE(v.sheettype, 'Unknown') AS content_type,
        CASE
            WHEN lvapc.last_viewed_at IS NULL
            THEN EXTRACT(DAY FROM (CURRENT_TIMESTAMP - w.created_at))::INTEGER
            ELSE EXTRACT(DAY FROM (CURRENT_TIMESTAMP - lvapc.last_viewed_at))::INTEGER
        END AS days_since_last_view,
        COALESCE(su.friendly_name, su.name, 'Unknown') AS owner
    FROM inactive_workbooks iw
    INNER JOIN workbooks w ON iw.workbook_id = w.id
    INNER JOIN views v ON v.workbook_id = w.id
    LEFT JOIN last_view_access_per_content lvapc ON v.id = lvapc.view_id
    LEFT JOIN users u ON w.owner_id = u.id
    LEFT JOIN system_users su ON u.system_user_id = su.id
),
ranked_content AS (
    SELECT
        workbook_name,
        content_name,
        content_type,
        days_since_last_view,
        owner,
        ROW_NUMBER() OVER (ORDER BY days_since_last_view DESC, workbook_name, content_type, content_name) AS rank
    FROM content_data
    ORDER BY days_since_last_view DESC, workbook_name, content_type, content_name
    LIMIT 50
)
SELECT
    rank,
    workbook_name,
    content_name,
    content_type,
    days_since_last_view,
    owner
FROM ranked_content
ORDER BY rank;
"""


def inactive_workbooks_summary_query() -> str:
    """Query for inactive workbooks summary (workbook level aggregation)."""
    return """
WITH recent_workbook_views AS (
    SELECT DISTINCT w.id AS workbook_id
    FROM workbooks w
    INNER JOIN views v ON v.workbook_id = w.id
    INNER JOIN hist_views hv ON hv.view_id = v.id
    INNER JOIN historical_events he ON he.hist_view_id = hv.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
),
last_view_per_workbook AS (
    SELECT
        w.id AS workbook_id,
        MAX(he.created_at) AS last_viewed_at
    FROM workbooks w
    INNER JOIN sites s ON w.site_id = s.id
    INNER JOIN views v ON v.workbook_id = w.id
    LEFT JOIN hist_views hv ON hv.view_id = v.id
    LEFT JOIN historical_events he ON he.hist_view_id = hv.id
    LEFT JOIN historical_event_types het
        ON he.historical_event_type_id = het.type_id
       AND het.name IN ('Access View', 'Access Authoring View')
    WHERE s.luid = %(site_id)s
    GROUP BY w.id
),
inactive_workbooks AS (
    SELECT DISTINCT w.id AS workbook_id
    FROM workbooks w
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND w.id NOT IN (SELECT workbook_id FROM recent_workbook_views)
),
workbook_data AS (
    SELECT
        w.name AS workbook_name,
        CASE
            WHEN lvpw.last_viewed_at IS NULL
            THEN EXTRACT(DAY FROM (CURRENT_TIMESTAMP - w.created_at))::INTEGER
            ELSE EXTRACT(DAY FROM (CURRENT_TIMESTAMP - lvpw.last_viewed_at))::INTEGER
        END AS days_since_last_view,
        COALESCE(su.friendly_name, su.name, 'Unknown') AS owner
    FROM inactive_workbooks iw
    INNER JOIN workbooks w ON iw.workbook_id = w.id
    LEFT JOIN last_view_per_workbook lvpw ON w.id = lvpw.workbook_id
    LEFT JOIN users u ON w.owner_id = u.id
    LEFT JOIN system_users su ON u.system_user_id = su.id
),
ranked_workbooks AS (
    SELECT
        workbook_name,
        days_since_last_view,
        owner,
        ROW_NUMBER() OVER (ORDER BY days_since_last_view DESC, workbook_name) AS rank
    FROM workbook_data
    ORDER BY days_since_last_view DESC, workbook_name
    LIMIT 50
)
SELECT
    rank,
    workbook_name,
    days_since_last_view,
    owner
FROM ranked_workbooks
ORDER BY rank;
"""


def content_creation_rate_query() -> str:
    return """
WITH month_series AS (
    SELECT 
        DATE_TRUNC('month', generate_series(
            DATE_TRUNC('month', CURRENT_TIMESTAMP - INTERVAL '12 months'),
            DATE_TRUNC('month', CURRENT_TIMESTAMP),
            INTERVAL '1 month'
        )) AS month_date
),
created_per_month AS (
    SELECT 
        DATE_TRUNC('month', w.created_at) AS month_date,
        COUNT(DISTINCT w.id) AS new_workbooks_created
    FROM workbooks w
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND w.created_at >= CURRENT_TIMESTAMP - INTERVAL '12 months'
    GROUP BY DATE_TRUNC('month', w.created_at)
),
modified_per_month AS (
    SELECT 
        DATE_TRUNC('month', w.updated_at) AS month_date,
        COUNT(DISTINCT w.id) AS workbooks_modified
    FROM workbooks w
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND w.updated_at >= CURRENT_TIMESTAMP - INTERVAL '12 months'
      AND DATE_TRUNC('month', w.updated_at) != DATE_TRUNC('month', w.created_at)
    GROUP BY DATE_TRUNC('month', w.updated_at)
),
examples AS (
    SELECT 
        DATE_TRUNC('month', w.created_at) AS month_date,
        STRING_AGG(w.name, ', ' ORDER BY w.created_at DESC) AS example_workbooks
    FROM (
        SELECT 
            w.created_at,
            w.name,
            ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', w.created_at) ORDER BY w.created_at DESC) AS rn
        FROM workbooks w
        INNER JOIN sites s ON w.site_id = s.id
        WHERE s.luid = %(site_id)s
          AND w.created_at >= CURRENT_TIMESTAMP - INTERVAL '12 months'
    ) w
    WHERE rn <= 2
    GROUP BY DATE_TRUNC('month', w.created_at)
)
SELECT 
    TO_CHAR(ms.month_date, 'Month YYYY') AS month,
    COALESCE(c.new_workbooks_created, 0) AS new_workbooks_created,
    COALESCE(m.workbooks_modified, 0) AS workbooks_modified,
    COALESCE(e.example_workbooks, 'N/A') AS example_of_new_content
FROM month_series ms
LEFT JOIN created_per_month c ON ms.month_date = c.month_date
LEFT JOIN modified_per_month m ON ms.month_date = m.month_date
LEFT JOIN examples e ON ms.month_date = e.month_date
ORDER BY ms.month_date DESC;
"""


def frequently_used_slow_reports_summary_query() -> str:
    """Query for frequently used slow reports summary (workbook level aggregation)."""
    return """
WITH view_load_times AS (
    SELECT 
        w.id AS workbook_id,
        w.name AS workbook_name,
        AVG(EXTRACT(EPOCH FROM (hr.completed_at - hr.created_at))) AS avg_load_time_seconds,
        COUNT(hr.id) AS http_request_count
    FROM http_requests hr
    INNER JOIN sites s ON hr.site_id = s.id
    INNER JOIN workbooks w ON w.site_id = s.id
        -- Try multiple matching strategies for workbook name
        AND (
            -- Strategy 1: Direct match
            SPLIT_PART(hr.currentsheet, '/', 1) = w.name
            -- Strategy 2: Match after removing numeric suffix only
            OR REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', '') = w.name
            -- Strategy 3: Match with all underscores converted to spaces
            OR REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), '_', ' ') = REPLACE(w.name, '_', ' ')
            -- Strategy 4: Match with all spaces converted to underscores (reverse)
            OR REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), ' ', '_') = REPLACE(w.name, ' ', '_')
            -- Strategy 5: Match ignoring all spaces and underscores (most flexible)
            OR REPLACE(REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), '_', ''), ' ', '') = 
               REPLACE(REPLACE(w.name, '_', ''), ' ', '')
        )
    WHERE 
        s.luid = %(site_id)s
        AND hr.currentsheet IS NOT NULL
        AND hr.currentsheet LIKE '%%/%%'
        AND hr.completed_at IS NOT NULL
        AND hr.created_at IS NOT NULL
        AND hr.completed_at > hr.created_at
        AND hr.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name
),
workbook_access_counts AS (
    SELECT 
        w.id AS workbook_id,
        w.name AS workbook_name,
        COUNT(he.id) AS view_count
    FROM historical_events he
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN hist_workbooks hw ON he.hist_workbook_id = hw.id
    INNER JOIN workbooks w ON hw.workbook_id = w.id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE 
        s.luid = %(site_id)s
        AND het.name IN ('Access View', 'Access Authoring View')
        AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name
),
workbook_data AS (
    SELECT
        COALESCE(vlt.workbook_name, vac.workbook_name) AS workbook_name,
        ROUND(vlt.avg_load_time_seconds::numeric, 2) AS avg_load_time_seconds,
        COALESCE(vac.view_count, 0) AS views_last_60_days
    FROM workbook_access_counts vac
    LEFT JOIN view_load_times vlt ON vac.workbook_id = vlt.workbook_id
    WHERE vlt.avg_load_time_seconds IS NOT NULL
       OR vac.view_count > 0
),
ranked_workbooks AS (
    SELECT
        workbook_name,
        avg_load_time_seconds,
        views_last_60_days,
        ROW_NUMBER() OVER (ORDER BY avg_load_time_seconds DESC NULLS LAST, views_last_60_days DESC) AS rank
    FROM workbook_data
    ORDER BY avg_load_time_seconds DESC NULLS LAST, views_last_60_days DESC
    LIMIT 50
)
SELECT
    rank,
    workbook_name,
    avg_load_time_seconds,
    views_last_60_days
FROM ranked_workbooks
ORDER BY rank;
"""


def frequently_used_slow_reports_detailed_query() -> str:
    """Query for frequently used slow reports detailed (view level granularity)."""
    return """
WITH view_load_times AS (
    SELECT 
        w.id AS workbook_id,
        w.name AS workbook_name,
        v.id AS view_id,
        v.name AS view_name,
        AVG(EXTRACT(EPOCH FROM (hr.completed_at - hr.created_at))) AS avg_load_time_seconds,
        COUNT(hr.id) AS http_request_count
    FROM http_requests hr
    INNER JOIN sites s ON hr.site_id = s.id
    INNER JOIN workbooks w ON w.site_id = s.id
        -- Try multiple matching strategies for workbook name
        AND (
            -- Strategy 1: Direct match
            SPLIT_PART(hr.currentsheet, '/', 1) = w.name
            -- Strategy 2: Match after removing numeric suffix only
            OR REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', '') = w.name
            -- Strategy 3: Match with all underscores converted to spaces
            OR REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), '_', ' ') = REPLACE(w.name, '_', ' ')
            -- Strategy 4: Match with all spaces converted to underscores (reverse)
            OR REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), ' ', '_') = REPLACE(w.name, ' ', '_')
            -- Strategy 5: Match ignoring all spaces and underscores (most flexible)
            OR REPLACE(REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), '_', ''), ' ', '') = 
               REPLACE(REPLACE(w.name, '_', ''), ' ', '')
        )
    INNER JOIN views v ON v.workbook_id = w.id
        -- Flexible view name matching: handle spaces, parentheses, and number variations
        AND (
            -- Strategy 1: Direct match
            SPLIT_PART(hr.currentsheet, '/', 2) = v.name
            -- Strategy 2: Match with spaces removed
            OR REPLACE(SPLIT_PART(hr.currentsheet, '/', 2), ' ', '') = REPLACE(v.name, ' ', '')
            -- Strategy 3: Match ignoring spaces, parentheses, and underscores
            OR REPLACE(REPLACE(REPLACE(SPLIT_PART(hr.currentsheet, '/', 2), ' ', ''), '(', ''), ')', '') = 
               REPLACE(REPLACE(REPLACE(v.name, ' ', ''), '(', ''), ')', '')
            -- Strategy 4: Match with number normalization (handle "2" vs "(2)" vs " 2")
            OR REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(SPLIT_PART(hr.currentsheet, '/', 2), ' ', ''), '(', ''), ')', ''), '([0-9]+)', '\\1') = 
               REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(v.name, ' ', ''), '(', ''), ')', ''), '([0-9]+)', '\\1')
        )
    WHERE 
        s.luid = %(site_id)s
        AND hr.currentsheet IS NOT NULL
        AND hr.currentsheet LIKE '%%/%%'
        AND hr.completed_at IS NOT NULL
        AND hr.created_at IS NOT NULL
        AND hr.completed_at > hr.created_at
        AND hr.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name, v.id, v.name
),
view_totals AS (
    SELECT
        w.id AS workbook_id,
        w.name AS workbook_name,
        v.id AS view_id,
        v.name AS view_name,
        COALESCE(v.sheettype, 'Unknown') AS content_type,
        COUNT(he.id) AS total_views
    FROM workbooks w
    INNER JOIN views v ON v.workbook_id = w.id
    INNER JOIN hist_views hv ON hv.view_id = v.id
    INNER JOIN historical_events he ON he.hist_view_id = hv.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name, v.id, v.name, v.sheettype
),
content_data AS (
    SELECT 
        vt.workbook_name,
        vt.view_name AS content_name,
        vt.content_type,
        ROUND(vlt.avg_load_time_seconds::numeric, 2) AS avg_load_time_seconds,
        vt.total_views AS views_last_60_days
    FROM view_totals vt
    LEFT JOIN view_load_times vlt 
        ON vt.workbook_name = vlt.workbook_name 
       AND vt.view_name = vlt.view_name
    WHERE vt.total_views > 0
),
ranked_content AS (
    SELECT
        workbook_name,
        content_name,
        content_type,
        avg_load_time_seconds,
        views_last_60_days,
        ROW_NUMBER() OVER (ORDER BY avg_load_time_seconds DESC NULLS LAST, views_last_60_days DESC, workbook_name, content_type, content_name) AS rank
    FROM content_data
    ORDER BY avg_load_time_seconds DESC NULLS LAST, views_last_60_days DESC, workbook_name, content_type, content_name
    LIMIT 50
)
SELECT
    rank,
    workbook_name,
    content_name,
    content_type,
    avg_load_time_seconds,
    views_last_60_days
FROM ranked_content
ORDER BY rank;
"""


def top_users_activity_query() -> str:
    return """
WITH user_actions AS (
    SELECT 
        COALESCE(su.id, hu.id) AS unique_user_id,
        COALESCE(su.friendly_name, su.name, hu.name) AS user_name,
        SUM(action_count) AS total_actions
    FROM (
        SELECT 
            he.hist_actor_user_id AS actor_id,
            COUNT(*) AS action_count
        FROM historical_events he
        WHERE he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
        GROUP BY he.hist_actor_user_id
    ) actions
    INNER JOIN hist_users hu ON actions.actor_id = hu.id
    LEFT JOIN system_users su ON hu.name = su.name
    GROUP BY COALESCE(su.id, hu.id), COALESCE(su.friendly_name, su.name, hu.name)
),
most_viewed_per_user AS (
    SELECT DISTINCT ON (COALESCE(su.id, hu.id))
        COALESCE(su.id, hu.id) AS unique_user_id,
        hv.name AS most_viewed_content,
        COUNT(*) AS view_count
    FROM historical_events he
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN hist_views hv ON he.hist_view_id = hv.id
    INNER JOIN hist_workbooks hw ON he.hist_workbook_id = hw.id
    INNER JOIN workbooks w ON hw.workbook_id = w.id
    INNER JOIN sites s ON w.site_id = s.id
    INNER JOIN hist_users hu ON he.hist_actor_user_id = hu.id
    LEFT JOIN system_users su ON hu.name = su.name
    WHERE s.luid = %(site_id)s
      AND het.name = 'Access View'
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY COALESCE(su.id, hu.id), hv.name
    ORDER BY COALESCE(su.id, hu.id), COUNT(*) DESC
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY ua.total_actions DESC) AS rank,
    ua.user_name,
    ua.total_actions AS actions_last_60_days,
    COALESCE(mv.most_viewed_content, 'No views') AS primary_focus_most_viewed
FROM user_actions ua
LEFT JOIN most_viewed_per_user mv ON ua.unique_user_id = mv.unique_user_id
ORDER BY ua.total_actions DESC;
"""


def developer_activity_query() -> str:
    return """
WITH developer_activity AS (
    SELECT 
        su.id AS system_user_id,
        COALESCE(su.friendly_name, su.name) AS user_name,
        COUNT(DISTINCT he.hist_workbook_id) AS workbooks_published
    FROM historical_events he
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN hist_users hu ON he.hist_actor_user_id = hu.id
    LEFT JOIN system_users su ON hu.name = su.name
    INNER JOIN hist_workbooks hw ON he.hist_workbook_id = hw.id
    INNER JOIN workbooks w ON hw.workbook_id = w.id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name = 'Publish Workbook'
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '300 days'
    GROUP BY su.id, su.friendly_name, su.name
    HAVING COUNT(DISTINCT he.hist_workbook_id) >= 1
),
example_workbooks_raw AS (
    SELECT 
        su.id AS system_user_id,
        hw.name AS workbook_name,
        he.created_at,
        ROW_NUMBER() OVER (PARTITION BY su.id ORDER BY he.created_at DESC) AS rn
    FROM historical_events he
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN hist_users hu ON he.hist_actor_user_id = hu.id
    LEFT JOIN system_users su ON hu.name = su.name
    INNER JOIN hist_workbooks hw ON he.hist_workbook_id = hw.id
    INNER JOIN workbooks w ON hw.workbook_id = w.id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name = 'Publish Workbook'
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '90 days'
),
example_workbooks AS (
    SELECT 
        system_user_id,
        STRING_AGG(workbook_name, ', ' ORDER BY created_at DESC) AS example_reports
    FROM example_workbooks_raw
    WHERE rn <= 2
    GROUP BY system_user_id
)
SELECT 
    COUNT(DISTINCT da.system_user_id) AS number_of_active_developers,
    STRING_AGG(DISTINCT ew.example_reports, '; ') AS example_reports_published
FROM developer_activity da
LEFT JOIN example_workbooks ew ON da.system_user_id = ew.system_user_id;
"""


def recommended_pilot_reports_query() -> str:
    return """
WITH workbook_load_times AS (
    SELECT 
        w.id AS workbook_id,
        w.name AS workbook_name,
        AVG(EXTRACT(EPOCH FROM (hr.completed_at - hr.created_at))) AS avg_load_time_seconds
    FROM http_requests hr
    INNER JOIN sites s ON hr.site_id = s.id
    INNER JOIN workbooks w ON w.site_id = s.id
        -- Try multiple matching strategies for workbook name
        AND (
            -- Strategy 1: Direct match
            SPLIT_PART(hr.currentsheet, '/', 1) = w.name
            -- Strategy 2: Match after removing numeric suffix only
            OR REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', '') = w.name
            -- Strategy 3: Match with all underscores converted to spaces
            OR REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), '_', ' ') = REPLACE(w.name, '_', ' ')
            -- Strategy 4: Match with all spaces converted to underscores (reverse)
            OR REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), ' ', '_') = REPLACE(w.name, ' ', '_')
            -- Strategy 5: Match ignoring all spaces and underscores (most flexible)
            OR REPLACE(REPLACE(REGEXP_REPLACE(SPLIT_PART(hr.currentsheet, '/', 1), '_[0-9]+$', ''), '_', ''), ' ', '') = 
               REPLACE(REPLACE(w.name, '_', ''), ' ', '')
        )
    WHERE s.luid = %(site_id)s
      AND hr.currentsheet IS NOT NULL
      AND hr.currentsheet LIKE '%%/%%'
      AND hr.completed_at IS NOT NULL
      AND hr.created_at IS NOT NULL
      AND hr.completed_at > hr.created_at
      AND hr.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name
),
workbook_usage AS (
    SELECT 
        w.id AS workbook_id,
        w.name AS workbook_name,
        COUNT(he.id) AS total_views
    FROM workbooks w
    INNER JOIN hist_workbooks hw ON hw.workbook_id = w.id
    INNER JOIN historical_events he ON he.hist_workbook_id = hw.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name
),
combined_metrics AS (
    SELECT 
        COALESCE(wu.workbook_id, wlt.workbook_id) AS workbook_id,
        COALESCE(wu.workbook_name, wlt.workbook_name) AS workbook_name,
        COALESCE(wu.total_views, 0) AS total_views,
        COALESCE(wlt.avg_load_time_seconds, 0) AS avg_load_time_seconds,
        CASE 
            WHEN COALESCE(wu.total_views, 0) >= 10 THEN 'High Usage'
            WHEN COALESCE(wu.total_views, 0) >= 5 THEN 'Moderate Usage'
            ELSE 'Low Usage'
        END AS usage_level,
        CASE 
            WHEN wlt.avg_load_time_seconds IS NULL THEN 'Unknown Load Time'
            WHEN wlt.avg_load_time_seconds > 10 THEN 'Slow Load Time'
            WHEN wlt.avg_load_time_seconds > 3 THEN 'Moderate Load Time'
            ELSE 'Fast Load Time'
        END AS performance_level
    FROM workbook_usage wu
    FULL OUTER JOIN workbook_load_times wlt ON wu.workbook_id = wlt.workbook_id
)
SELECT 
    workbook_name AS report_name,
    usage_level || ', ' || performance_level AS reason_for_selection
FROM combined_metrics
WHERE COALESCE(total_views, 0) >= 5  -- Only High Usage (>=10) or Moderate Usage (>=5), load time can be anything
ORDER BY total_views DESC, avg_load_time_seconds DESC;
"""


def quick_wins_scatter_data_query() -> str:
    return """
WITH workbook_load_times AS (
    SELECT 
        w.id AS workbook_id,
        w.name AS workbook_name,
        AVG(EXTRACT(EPOCH FROM (hr.completed_at - hr.created_at))) AS avg_load_time_seconds
    FROM http_requests hr
    INNER JOIN sites s ON hr.site_id = s.id
    INNER JOIN workbooks w ON SPLIT_PART(hr.currentsheet, '/', 1) = w.name AND w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND hr.currentsheet IS NOT NULL
      AND hr.currentsheet LIKE '%%/%%'
      AND hr.completed_at IS NOT NULL
      AND hr.created_at IS NOT NULL
      AND hr.completed_at > hr.created_at
      AND hr.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name
),
workbook_usage AS (
    SELECT 
        w.id AS workbook_id,
        w.name AS workbook_name,
        COUNT(he.id) AS total_views
    FROM workbooks w
    INNER JOIN hist_workbooks hw ON hw.workbook_id = w.id
    INNER JOIN historical_events he ON he.hist_workbook_id = hw.id
    INNER JOIN historical_event_types het ON he.historical_event_type_id = het.type_id
    INNER JOIN sites s ON w.site_id = s.id
    WHERE s.luid = %(site_id)s
      AND het.name IN ('Access View', 'Access Authoring View')
      AND he.created_at >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY w.id, w.name
),
combined_metrics AS (
    SELECT 
        COALESCE(wu.workbook_id, wlt.workbook_id) AS workbook_id,
        COALESCE(wu.workbook_name, wlt.workbook_name) AS workbook_name,
        COALESCE(wu.total_views, 0) AS total_views,
        COALESCE(wlt.avg_load_time_seconds, 0) AS avg_load_time_seconds
    FROM workbook_usage wu
    FULL OUTER JOIN workbook_load_times wlt ON wu.workbook_id = wlt.workbook_id
    WHERE COALESCE(wu.total_views, 0) > 0 OR COALESCE(wlt.avg_load_time_seconds, 0) > 0
)
SELECT 
    cm.workbook_name,
    cm.total_views AS views_60_days,
    ROUND(cm.avg_load_time_seconds::numeric, 2) AS avg_load_time_seconds
FROM combined_metrics cm
ORDER BY cm.total_views DESC, cm.avg_load_time_seconds DESC;
"""


def build_output_json(
    results: Dict[str, List[Dict[str, Any]]], 
    site_id: str,
    calculated_metrics: Dict[str, Any],
    descriptions: Dict[str, str]
) -> Dict[str, Any]:
    """Assemble final JSON structure with calculated metrics and descriptions."""
    return {
        "site_id": site_id,
        "calculated_metrics": calculated_metrics,
        "table_descriptions": descriptions,
        "Usage_Statistics": {
            "Most_Used_Workbooks_Summary": rows_to_indexed_map(
                results["most_used_workbooks_summary"]
            ),
            "Most_Used_Workbooks_Detailed": rows_to_indexed_map(
                results["most_used_workbooks_detailed"]
            ),
            "Inactive_Workbooks_Summary": rows_to_indexed_map(
                results["inactive_workbooks_summary"]
            ),
            "Inactive_Workbooks_Detailed": rows_to_indexed_map(
                results["inactive_workbooks_detailed"]
            ),
            "Content_Creation_Rate": rows_to_indexed_map(
                results["content_creation_rate"]
            ),
        },
        "User_Statistics": {
            "Top_Users_Activity": rows_to_indexed_map(results["top_users_activity"]),
            "Developer_Activity": rows_to_indexed_map(results["developer_activity"]),
        },
        "Performance_Statistics": {
            "Frequently_Used_Slow_Reports_Summary": rows_to_indexed_map(results["frequently_used_slow_reports_summary"]),
            "Frequently_Used_Slow_Reports_Detailed": rows_to_indexed_map(results["frequently_used_slow_reports_detailed"]),
        },
        "Quick_Wins_For_Migration": {
            "Recommended_Pilot_Reports": rows_to_indexed_map(results["recommended_pilot_reports"]),
            "Quick_Wins_Scatter_Data": rows_to_indexed_map(results["quick_wins_scatter_data"]),
        }
    }


def write_output_json(payload: Dict[str, Any], site_id: str, output_dir: str = ".") -> str:
    """Write metrics JSON to file. Returns the full path to the written file."""
    output_path = Path(output_dir) if output_dir else Path(".")
    output_path.mkdir(parents=True, exist_ok=True)
    filename = output_path / f"metrics_{site_id}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return str(filename)


def generate_metrics_json(
    site_id: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    pg_user: str,
    pg_password: str,
    pg_sslmode: str = "prefer",
    pg_connect_timeout: int = 15,
    output_dir: str = "output"
) -> str:
    """
    Generate metrics JSON from PostgreSQL database.
    
    Args:
        site_id: Tableau site LUID (fetched from Tableau API)
        pg_host: PostgreSQL host (required)
        pg_port: PostgreSQL port (required)
        pg_db: PostgreSQL database name (required)
        pg_user: PostgreSQL user (required)
        pg_password: PostgreSQL password (required)
        pg_sslmode: PostgreSQL SSL mode (default: "prefer")
        pg_connect_timeout: PostgreSQL connection timeout in seconds (default: 15)
        output_dir: Directory to save the metrics JSON file (default: "output")
        
    Returns:
        str: Path to the generated metrics JSON file
        
    Raises:
        ValueError: If any required parameter is missing
    """
    # Validate required parameters
    if not site_id:
        raise ValueError("site_id is required")
    if not pg_host:
        raise ValueError("pg_host is required")
    if not pg_port:
        raise ValueError("pg_port is required")
    if not pg_db:
        raise ValueError("pg_db is required")
    if not pg_user:
        raise ValueError("pg_user is required")
    if not pg_password:
        raise ValueError("pg_password is required")
    
    # Create database configuration from provided values
    db_cfg = DbConfig(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
        sslmode=pg_sslmode,
        connect_timeout=pg_connect_timeout,
    )

    print("Connecting to PostgreSQL...")
    conn = pg_connect(db_cfg)
    print("Running PostgreSQL metrics...")
    queries = {
        "most_used_workbooks_summary": most_used_workbooks_summary_query(),
        "most_used_workbooks_detailed": most_used_workbooks_detailed_query(),
        "inactive_workbooks_summary": inactive_workbooks_summary_query(),
        "inactive_workbooks_detailed": inactive_workbooks_detailed_query(),
        "content_creation_rate": content_creation_rate_query(),
        "frequently_used_slow_reports_summary": frequently_used_slow_reports_summary_query(),
        "frequently_used_slow_reports_detailed": frequently_used_slow_reports_detailed_query(),
        "top_users_activity": top_users_activity_query(),
        "developer_activity": developer_activity_query(),
        "recommended_pilot_reports": recommended_pilot_reports_query(),
        "quick_wins_scatter_data": quick_wins_scatter_data_query(),
    }

    results: Dict[str, List[Dict[str, Any]]] = {}
    for key, sql in queries.items():
        print(f"Executing query: {key}")
        try:
            results[key] = run_query(sql, site_id, conn)
            print(f" - rows returned: {len(results[key])}")
        except Exception as exc:  # noqa: BLE001
            err_msg = f"{exc}"
            print(f" - FAILED: {err_msg}")
            # Record the error in the output so JSON still generates.
            results[key] = [{"error": err_msg}]

    # Get total workbooks count and inactive workbooks count for calculations
    print("Calculating metrics...")
    total_workbooks = 0
    try:
        total_result = run_query(total_workbooks_query(), site_id, conn)
        if total_result and "error" not in total_result[0]:
            total_workbooks = int(total_result[0].get("total_workbooks", 0))
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: Could not get total workbooks count: {exc}")

    inactive_workbooks_count = 0
    try:
        inactive_count_result = run_query(inactive_workbooks_count_query(), site_id, conn)
        if inactive_count_result and "error" not in inactive_count_result[0]:
            inactive_workbooks_count = int(inactive_count_result[0].get("inactive_workbooks_count", 0))
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: Could not get inactive workbooks count: {exc}")

    # Calculate all metrics
    calculated_metrics = {
        "inactive_workbooks_summary": calculate_inactive_workbooks_metrics(
            inactive_workbooks_count,
            total_workbooks
        ),
        "content_creation_rate": calculate_content_creation_rate_metrics(
            results.get("content_creation_rate", [])
        ),
        "developer_activity": calculate_developer_activity_metrics(
            results.get("developer_activity", [])
        ),
    }

    # Generate descriptions based on calculated metrics and results
    descriptions = generate_table_descriptions(calculated_metrics, results)

    payload = build_output_json(results, site_id, calculated_metrics, descriptions)
    outfile = write_output_json(payload, site_id, output_dir)
    print(f"Wrote metrics to {outfile}")
    conn.close()
    return outfile


# Note: This module is designed to be called from main.py via generate_metrics_json()
# All credentials must be provided via command line arguments - no defaults or env variables
