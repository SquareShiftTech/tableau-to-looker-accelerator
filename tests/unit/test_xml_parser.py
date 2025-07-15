"""Unit tests for the XML parser."""

import pytest
from lxml import etree as ET

from tableau_to_looker_parser.core.xml_parser import TableauXMLParser
from tableau_to_looker_parser.models.xml_models import (
    WorkbookModel,
    ColumnModel,
    PhysicalRelationshipModel,
    LogicalRelationshipModel,
)


@pytest.fixture
def parser():
    """Create a fresh parser for each test."""
    return TableauXMLParser()


@pytest.fixture
def column_xml():
    """Sample column XML."""
    return """
    <column name="[Sales]" 
            role="measure" 
            datatype="real" 
            type="quantitative"
            aggregation="sum">
        <calculation formula="[Price] * [Quantity]"/>
    </column>
    """


@pytest.fixture
def physical_join_xml():
    """Sample physical join XML."""
    return """
    <relation join="inner" type="join">
        <clause type="join">
            <expression op="=">
                <expression op="[table1].[id]"/>
                <expression op="[table2].[id]"/>
            </expression>
        </clause>
        <relation connection="conn1" name="table1" 
                 table="[db].[table1]" type="table"/>
        <relation connection="conn1" name="table2"
                 table="[db].[table2]" type="table"/>
    </relation>
    """


@pytest.fixture
def logical_relationship_xml():
    """Sample logical relationship XML."""
    return """
    <object-graph>
        <objects>
            <object id="obj1" caption="Table1">
                <properties>
                    <relation connection="conn1" name="table1"
                             table="[db].[table1]"/>
                </properties>
            </object>
            <object id="obj2" caption="Table2">
                <properties>
                    <relation connection="conn1" name="table2"
                             table="[db].[table2]"/>
                </properties>
            </object>
        </objects>
        <relationships>
            <relationship>
                <expression op="=">
                    <expression op="[id1]"/>
                    <expression op="[id2]"/>
                </expression>
                <first-end-point object-id="obj1"/>
                <second-end-point object-id="obj2"/>
            </relationship>
        </relationships>
    </object-graph>
    """


def test_parse_column(parser, column_xml):
    """Test parsing a column element."""
    element = ET.fromstring(column_xml)
    column = parser._extract_column(element)

    assert isinstance(column, ColumnModel)
    assert column.name == "[Sales]"
    assert column.role == "measure"
    assert column.datatype == "real"
    assert column.type == "quantitative"
    assert column.aggregation == "sum"
    assert column.calculation == "[Price] * [Quantity]"


def test_parse_physical_join(parser, physical_join_xml):
    """Test parsing a physical join."""
    element = ET.fromstring(physical_join_xml)
    join = parser._extract_physical_join(element)

    assert isinstance(join, PhysicalRelationshipModel)
    assert join.join_type == "inner"
    assert len(join.tables) == 2
    assert join.tables[0].table == "[db].[table1]"
    assert join.tables[1].table == "[db].[table2]"
    assert join.expression.operator == "="
    assert "[table1].[id]" in join.expression.expressions
    assert "[table2].[id]" in join.expression.expressions


def test_parse_logical_relationship(parser, logical_relationship_xml):
    """Test parsing a logical relationship."""
    element = ET.fromstring(logical_relationship_xml)
    rel = parser._extract_logical_relationship(element)

    assert isinstance(rel, LogicalRelationshipModel)
    assert rel.first_endpoint.table == "[db].[table1]"
    assert rel.second_endpoint.table == "[db].[table2]"
    assert rel.expression.operator == "="
    assert "[id1]" in rel.expression.expressions
    assert "[id2]" in rel.expression.expressions


def test_parse_workbook(parser, tmp_path):
    """Test parsing a complete workbook."""
    # Create a minimal workbook
    workbook = f"""<?xml version='1.0' encoding='utf-8' ?>
    <workbook version="18.1">
        <datasources>
            <datasource name="test">
                {column_xml}
                {physical_join_xml}
                {logical_relationship_xml}
            </datasource>
        </datasources>
    </workbook>
    """

    # Write to temp file
    workbook_path = tmp_path / "test.twb"
    workbook_path.write_text(workbook)

    # Parse workbook
    result = parser.parse_file(workbook_path)

    # Validate result
    assert isinstance(result, WorkbookModel)
    assert len(result.elements.columns) == 1
    assert len(result.elements.relationships) == 2
