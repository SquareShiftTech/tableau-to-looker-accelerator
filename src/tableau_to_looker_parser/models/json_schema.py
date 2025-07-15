from enum import Enum
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field, ConfigDict


class DatabaseType(str, Enum):
    """Supported database types"""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLSERVER = "sqlserver"
    ORACLE = "oracle"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    FEDERATED = "federated"  # Federated data source


class AuthenticationType(str, Enum):
    """Supported authentication types"""

    USERNAME_PASSWORD = "username-password"
    OAUTH = "oauth"
    SERVICE_ACCOUNT = "service-account"
    IAM = "iam"
    WINDOWS_AUTH = "windows-auth"
    NONE = "none"


class BaseConnectionSchema(BaseModel):
    """Base schema for all connection types"""

    type: DatabaseType
    name: str


class StandardConnectionSchema(BaseConnectionSchema):
    """Schema for standard database connections"""

    server: str
    database: str
    port: Optional[int] = None
    username: Optional[str] = None
    db_schema: Optional[str] = None
    ssl_enabled: Optional[bool] = None
    authentication: Optional[AuthenticationType] = None
    properties: Dict[str, str] = Field(default_factory=dict)

    @property
    def type(self) -> DatabaseType:
        """Override to ensure standard connections don't use special types"""
        if not hasattr(self, "_type"):
            raise ValueError("type must be set")
        if self._type in [DatabaseType.BIGQUERY, DatabaseType.FEDERATED]:
            raise ValueError(
                "Standard connections cannot use BIGQUERY or FEDERATED types"
            )
        return self._type

    @type.setter
    def type(self, value: DatabaseType):
        if value in [DatabaseType.BIGQUERY, DatabaseType.FEDERATED]:
            raise ValueError(
                "Standard connections cannot use BIGQUERY or FEDERATED types"
            )
        self._type = value


class BigQueryConnectionSchema(BaseConnectionSchema):
    """Schema for BigQuery connections"""

    project: Optional[str] = None
    dataset: Optional[str] = None
    service_account: Optional[str] = None
    authentication: AuthenticationType = AuthenticationType.SERVICE_ACCOUNT
    properties: Dict[str, str] = Field(default_factory=dict)

    def __init__(self, **data):
        if "type" in data and data["type"] != DatabaseType.BIGQUERY:
            raise ValueError("BigQuery connections must use type=DatabaseType.BIGQUERY")
        super().__init__(**data)
        self.type = DatabaseType.BIGQUERY


class FederatedConnectionSchema(BaseConnectionSchema):
    """Schema for federated data sources"""

    connections: List[Union[StandardConnectionSchema, BigQueryConnectionSchema]] = (
        Field(default_factory=list)
    )
    primary_connection: Optional[str] = None  # Name of primary connection

    def __init__(self, **data):
        if "type" in data and data["type"] != DatabaseType.FEDERATED:
            raise ValueError(
                "Federated connections must use type=DatabaseType.FEDERATED"
            )
        super().__init__(**data)
        self.type = DatabaseType.FEDERATED

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "type": "federated",
                "name": "my_federated_source",
                "connections": [
                    {
                        "type": "postgresql",
                        "name": "pg_source",
                        "server": "localhost",
                        "database": "mydb",
                    },
                    {
                        "type": "bigquery",
                        "name": "bq_source",
                        "project": "myproject",
                        "dataset": "mydataset",
                    },
                ],
                "primary_connection": "bq_source",
            }
        }
    )


ConnectionSchema = Union[
    StandardConnectionSchema, BigQueryConnectionSchema, FederatedConnectionSchema
]


class DimensionType(str, Enum):
    """Supported dimension types"""

    STRING = "string"
    INTEGER = "integer"
    REAL = "real"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"


class TimeframeType(str, Enum):
    """Time granularities for date dimensions"""

    RAW = "raw"
    DATE = "date"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class ParameterType(str, Enum):
    """Supported parameter types"""

    RANGE = "range"  # Numeric range with min/max/step
    LIST = "list"  # List of allowed values
    DATE = "date"  # Date parameter with optional range
    STRING = "string"  # Free-form string
    NUMBER = "number"  # Single number
    BOOLEAN = "boolean"  # True/false
    DATETIME = "datetime"  # Date and time


class RangeParameterSettings(BaseModel):
    """Range parameter with min/max/step.

    Used for both numeric and date ranges.
    For dates, values should be in ISO format: YYYY-MM-DD
    """

    min: str
    max: str
    granularity: Optional[str] = (
        None  # Step size for numbers, interval for dates (day,week,month,year)
    )
    inclusive_min: bool = True  # Whether min value is included
    inclusive_max: bool = True  # Whether max value is included


class ListParameterSettings(BaseModel):
    """List parameter with allowed values.

    For string lists: ['North', 'South', 'East', 'West']
    For number lists: ['1', '2', '3', '4']
    For date lists: ['2024-01-01', '2024-06-01', '2024-12-31']
    """

    values: List[str]
    allow_multiple: bool = False  # Whether multiple values can be selected
    value_type: str = "string"  # Type of values: string, number, date


class DateParameterSettings(BaseModel):
    """Date parameter specific settings."""

    format: str = "YYYY-MM-DD"  # Date format
    range: Optional[RangeParameterSettings] = None  # Optional date range
    allowed_values: Optional[ListParameterSettings] = None  # Optional specific dates


class ParameterSettings(BaseModel):
    """Parameter configuration.

    Each parameter type has its own validation requirements:
    - RANGE: requires range settings
    - LIST: requires list settings
    - DATE: requires date settings
    - STRING: optional list for allowed values
    - NUMBER: optional range or list
    - BOOLEAN: no additional settings
    - DATETIME: requires date settings
    """

    type: ParameterType
    default_value: Optional[str] = None
    description: Optional[str] = None
    required: bool = False
    # Type-specific settings
    range: Optional[RangeParameterSettings] = None
    list: Optional[ListParameterSettings] = None
    date: Optional[DateParameterSettings] = None


class ExpressionSchema(BaseModel):
    """Schema for join expressions"""

    operator: str
    expressions: List[Union[str, "ExpressionSchema"]]


class TableSchema(BaseModel):
    """Schema for table in a physical join"""

    connection: str
    name: str
    table: str


class EndpointSchema(BaseModel):
    """Schema for endpoint in a logical relationship"""

    object_id: str
    caption: Optional[str] = None
    table: Optional[str] = None
    connection: Optional[str] = None


class LogicalRelationshipSchema(BaseModel):
    """Schema for logical relationships between datasources"""

    relationship_type: str = "logical"
    expression: ExpressionSchema
    first_endpoint: EndpointSchema
    second_endpoint: EndpointSchema


class PhysicalJoinSchema(BaseModel):
    """Schema for physical join between tables"""

    relationship_type: str = "physical"
    join_type: str
    expression: ExpressionSchema
    tables: List[TableSchema]


RelationshipSchema = Union[LogicalRelationshipSchema, PhysicalJoinSchema]


class DimensionSchema(BaseModel):
    """Schema for dimension fields"""

    name: str
    field_type: DimensionType
    label: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    timeframes: Optional[List[TimeframeType]] = None
    sql: Optional[str] = None
    group_label: Optional[str] = None
    calculation: Optional[str] = None  # Calculated field formula


class AggregationType(str, Enum):
    """Supported aggregation types"""

    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


class MeasureSchema(BaseModel):
    """Schema for measure fields"""

    name: str
    aggregation: AggregationType
    label: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    sql: Optional[str] = None
    value_format: Optional[str] = None
    group_label: Optional[str] = None


class ParameterSchema(BaseModel):
    """Schema for parameter fields"""

    name: str
    field_type: DimensionType
    label: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    parameter: ParameterSettings


class ViewSchema(BaseModel):
    """Schema for LookML view"""

    name: str
    label: Optional[str] = None
    derived_table: Optional[str] = None
    dimensions: List[DimensionSchema] = Field(default_factory=list)
    measures: List[MeasureSchema] = Field(default_factory=list)
    parameters: List[ParameterSchema] = Field(default_factory=list)
    source_table: Optional[str] = None
    relationships: List[RelationshipSchema] = Field(default_factory=list)


class ModelSchema(BaseModel):
    """Root schema for migrated LookML model"""

    connection: Union[
        StandardConnectionSchema, BigQueryConnectionSchema, FederatedConnectionSchema
    ]
    views: List[ViewSchema]
    explores: List[Dict] = Field(default_factory=list)  # Will be expanded later

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "connection": {
                    "type": "postgresql",
                    "name": "my_db",
                    "server": "localhost",
                    "database": "mydatabase",
                    "port": 5432,
                },
                "views": [
                    {
                        "name": "orders",
                        "dimensions": [
                            {
                                "name": "order_date",
                                "field_type": "date",
                                "timeframes": ["raw", "month", "year"],
                            }
                        ],
                        "measures": [
                            {
                                "name": "total_revenue",
                                "aggregation": "sum",
                                "value_format": "$#,##0.00",
                            }
                        ],
                    }
                ],
            }
        }
    )
