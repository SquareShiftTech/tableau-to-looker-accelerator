"""XML data models for Tableau workbook parsing.

Models the raw data structures extracted from Tableau XML,
before any business logic or transformation is applied.
"""

from typing import List, Optional
from pydantic import BaseModel, Field


class CalculationModel(BaseModel):
    """Raw calculation from XML."""

    formula: str = Field(description="The calculation formula")


class ColumnModel(BaseModel):
    """Raw column definition from XML."""

    name: str = Field(description="Column name")
    role: str = Field(description="Column role (dimension/measure)")
    datatype: str = Field(description="Data type")
    semantic_role: Optional[str] = Field(None, description="Semantic role")
    caption: Optional[str] = Field(None, description="Display caption")
    type: Optional[str] = Field(None, description="Column type")
    aggregation: Optional[str] = Field(None, description="Aggregation type")
    calculation: Optional[str] = Field(
        None, description="Calculation formula if present"
    )


class ExpressionModel(BaseModel):
    """Raw join expression from XML."""

    operator: str = Field(description="Expression operator")
    expressions: List[str] = Field(description="Expression parts")


class TableReferenceModel(BaseModel):
    """Raw table reference from XML."""

    connection: str = Field(description="Connection name")
    name: str = Field(description="Table name")
    table: str = Field(description="Full table path")


class PhysicalRelationshipModel(BaseModel):
    """Raw physical join from XML."""

    type: str = Field("physical", description="Always 'physical'")
    join_type: str = Field(description="Join type (inner/left/right)")
    tables: List[TableReferenceModel] = Field(description="Tables being joined")
    expression: ExpressionModel = Field(description="Join expression")


class EndpointModel(BaseModel):
    """Raw relationship endpoint from XML."""

    object_id: str = Field(description="Object ID")
    caption: Optional[str] = Field(None, description="Display caption")
    connection: Optional[str] = Field(None, description="Connection name")
    name: Optional[str] = Field(None, description="Table name")
    table: Optional[str] = Field(None, description="Full table path")


class LogicalRelationshipModel(BaseModel):
    """Raw logical relationship from XML."""

    type: str = Field("logical", description="Always 'logical'")
    expression: ExpressionModel = Field(description="Relationship expression")
    first_endpoint: EndpointModel = Field(description="First endpoint")
    second_endpoint: EndpointModel = Field(description="Second endpoint")


class ConnectionModel(BaseModel):
    """Raw connection from XML."""

    class_: str = Field(alias="class", description="Connection class")
    dbname: str = Field(description="Database name")
    server: str = Field(description="Server name")
    username: str = Field(description="Username")
    authentication: str = Field(description="Authentication type")


class WorkbookMetadataModel(BaseModel):
    """Raw workbook metadata from XML."""

    source_file: str = Field(description="Source file path")
    workbook_version: str = Field(description="Workbook version")
    original_version: str = Field(description="Original version")


class WorkbookElementsModel(BaseModel):
    """All raw elements extracted from workbook XML."""

    columns: List[ColumnModel] = Field(default_factory=list)
    relationships: List[PhysicalRelationshipModel | LogicalRelationshipModel] = Field(
        default_factory=list
    )
    connections: List[ConnectionModel] = Field(default_factory=list)


class WorkbookModel(BaseModel):
    """Complete workbook data extracted from XML."""

    metadata: WorkbookMetadataModel
    elements: WorkbookElementsModel
