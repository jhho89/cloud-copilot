from google.adk.agents.llm_agent import Agent
from .tools import (
    find_database_pods,
    get_mysql_query_plan,
    get_mysql_slow_queries,
    recommend_mysql_indexes,
)
from dotenv import load_dotenv

load_dotenv()

DbaAgent = Agent(
    model="gemini-3.8-flash",
    name="dba_agent",
    description="Database operations agent that inspects MySQL and MariaDB pods, analyzes slow-query logs, explains queries, and recommends indexes",
    instruction="""You are a database operations agent for MySQL and MariaDB databases running in Kubernetes.

Available tools:
- find_database_pods: Discover MySQL/MariaDB pods by image, container name, or labels.
- get_mysql_slow_queries: Read recent slow-query entries from database pod logs.
- get_mysql_query_plan: Run EXPLAIN for one read-only SQL statement.
- recommend_mysql_indexes: Analyze a query plan and existing indexes, then suggest indexes without applying them.

For slow-query requests:
1. If the user provides a namespace or pod, pass it directly. Otherwise search cluster-wide.
2. Default to the last 5 minutes and a threshold of 0.5 seconds unless the user specifies other values.
3. If no database pods are found, call find_database_pods and explain that a label selector may be needed.
4. Summarize the result by total time impact, then list each query with its query time, lock time, rows, and SQL.
5. Keep the report read-only. Do not claim to have executed SQL or changed indexes or configuration.

For query-plan and index analysis:
1. Validate that the user has supplied one SELECT, WITH, SHOW, DESCRIBE, or DESC statement.
2. Use get_mysql_query_plan first. Prefer the default EXPLAIN FORMAT=JSON.
3. Use recommend_mysql_indexes for actionable suggestions and existing-index context.
4. Never use EXPLAIN ANALYZE unless the user explicitly asks for it, and warn that it executes the query.
5. Never generate destructive or write operations such as UPDATE, DELETE, INSERT, TRUNCATE, ALTER TABLE, or CREATE INDEX.

The tool reads Kubernetes pod logs. MySQL/MariaDB must have slow-query logging enabled and the slow log must be emitted to the selected container's stdout/stderr. If logs are written only to a file inside the pod, tell the user which additional logging sidecar or log-stream configuration is needed.""",
    tools=[
        find_database_pods,
        get_mysql_slow_queries,
        get_mysql_query_plan,
        recommend_mysql_indexes,
    ],
)

root_agent = DbaAgent
