from typing import Dict, Optional
from tableau_to_looker_parser.handlers.base_handler import BaseHandler
from tableau_to_looker_parser.models.json_schema import (
    DatabaseType,
    AuthenticationType,
    StandardConnectionSchema,
    BigQueryConnectionSchema,
    FederatedConnectionSchema,
)


class ConnectionHandler(BaseHandler):
    """Handler for Tableau connection data.

    Handles:
    - Converting raw connection data to standardized JSON format
    - Mapping connection types
    - Authentication type standardization
    - Connection property organization
    - Federated connection standardization

    Does NOT handle XML parsing - that's XMLParser's job.
    """

    # Map Tableau connection classes to our types
    TYPE_MAP = {
        "bigquery": DatabaseType.BIGQUERY,
        "mysql": DatabaseType.MYSQL,
        "postgresql": DatabaseType.POSTGRESQL,
        "sqlserver": DatabaseType.SQLSERVER,
        "oracle": DatabaseType.ORACLE,
        "snowflake": DatabaseType.SNOWFLAKE,
    }

    def can_handle(self, data: Dict) -> float:
        """Determine if this handler can process the raw data.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            float: Confidence score between 0.0 and 1.0
        """
        conn_class = data.get("class", "").lower()

        # Handle federated connections
        if conn_class == "federated" or data.get("type") == "federated":
            return 1.0

        # Handle standard database connections
        if conn_class in self.TYPE_MAP:
            return 1.0

        # Other connection types
        if conn_class:
            return 0.5

        return 0.0

    def _determine_auth_type(self, data: Dict) -> Optional[AuthenticationType]:
        """Determine authentication type from connection data.

        Args:
            data: Raw connection data from XMLParser

        Returns:
            AuthenticationType or None
        """
        auth = data.get("authentication")
        class_type = data.get("class")

        if class_type == "bigquery":
            # BigQuery typically uses service account
            return AuthenticationType.SERVICE_ACCOUNT

        if auth == "oauth":
            return AuthenticationType.OAUTH
        elif auth == "integrated":
            return AuthenticationType.WINDOWS_AUTH
        elif auth in ["username-password", "auth-user-pass"]:
            return AuthenticationType.USERNAME_PASSWORD

        return None

    def convert_to_json(self, data: Dict) -> Dict:
        """Convert raw connection data to schema-compliant JSON.

        Args:
            raw_data: Raw data dict from XMLParser.extract_connection()

        Returns:
            Dict: Schema-compliant connection data
        """
        # Determine connection type
        conn_class = data.get("class", "").lower()
        conn_type = self.TYPE_MAP.get(
            conn_class, DatabaseType.POSTGRESQL
        )  # Default to PostgreSQL

        # Extract standard properties
        properties = {}
        for key, value in data.get("metadata", {}).items():
            if key not in ["name", "server", "dbname", "username", "port", "schema"]:
                properties[key] = value

        # Get authentication type
        auth_type = self._determine_auth_type(data)

        if conn_class == "bigquery":
            # Generate meaningful connection name
            connection_name = data.get("name", "")
            if not connection_name:
                # Use caption if available, otherwise generate from dataset
                caption = data.get("caption", "")
                dataset = data.get("schema", "")
                if caption:
                    connection_name = f"bigquery_{caption.lower().replace(' ', '_')}"
                elif dataset:
                    connection_name = f"bigquery_{dataset.lower()}"
                else:
                    connection_name = "bigquery_default"

            # Create BigQuery connection
            conn = BigQueryConnectionSchema(
                type=DatabaseType.BIGQUERY,
                name=connection_name,
                project=data.get("metadata", {}).get("project"),
                dataset=data.get("schema"),
                service_account=data.get("username"),
                authentication=auth_type or AuthenticationType.SERVICE_ACCOUNT,
            )
        elif conn_class == "federated":
            # Create federated connection
            sub_connections = []
            for sub_data in data.get("connections", []):
                if sub_data.get("class") == "bigquery":
                    sub_conn = BigQueryConnectionSchema(
                        type=DatabaseType.BIGQUERY,
                        name=sub_data["name"],
                        project=sub_data.get("metadata", {}).get("project"),
                        dataset=sub_data.get("schema"),
                        service_account=sub_data.get("username"),
                        authentication=auth_type or AuthenticationType.SERVICE_ACCOUNT,
                    )
                else:
                    sub_conn = StandardConnectionSchema(
                        type=self.TYPE_MAP.get(
                            sub_data.get("class", "").lower(), DatabaseType.POSTGRESQL
                        ),
                        name=sub_data["name"],
                        server=sub_data["server"],
                        database=sub_data["dbname"],
                        port=sub_data.get("port"),
                        username=sub_data.get("username"),
                        db_schema=sub_data.get("schema"),
                        authentication=self._determine_auth_type(sub_data),
                        properties=sub_data.get("metadata", {}),
                    )
                sub_connections.append(sub_conn)

            conn = FederatedConnectionSchema(
                type=DatabaseType.FEDERATED,
                name=data["name"],
                connections=sub_connections,
                primary_connection=data.get(
                    "workgroup"
                ),  # Use workgroup as primary connection
            )
        else:
            # Create standard connection
            conn = StandardConnectionSchema(
                type=conn_type,
                name=data["name"],
                server=data["server"],
                database=data["dbname"],
                port=data.get("port"),
                username=data.get("username"),
                db_schema=data.get("schema"),
                authentication=auth_type,
                properties=properties,
            )

        return conn.model_dump()
