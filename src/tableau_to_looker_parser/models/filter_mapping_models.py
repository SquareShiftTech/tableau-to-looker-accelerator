"""
Pydantic models for Tableau to LookML filter mapping.

Clean, type-safe mapping system with validation.
"""

from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field, validator
from enum import Enum


class TableauFilterType(str, Enum):
    """Tableau filter types."""

    FILTER_DEFINITION = "filter_definition"
    WORKSHEET_CARD = "worksheet_card"


class TableauFilterClass(str, Enum):
    """Tableau filter classes."""

    CATEGORICAL = "categorical"
    RELATIVE_DATE = "relative-date"
    QUANTITATIVE = "quantitative"


class GroupfilterFunction(str, Enum):
    """Tableau groupfilter functions."""

    LEVEL_MEMBERS = "level-members"
    MEMBER = "member"
    UNION = "union"
    CROSSJOIN = "crossjoin"
    EXCEPT = "except"
    ORDER = "order"


class LookMLFilterType(str, Enum):
    """LookML element filter types."""

    FIELD_FILTER = "field_filter"
    DATE_FILTER = "date_filter"
    NUMBER_FILTER = "number_filter"


class GroupfilterLogic(BaseModel):
    """Tableau groupfilter logic structure."""

    function: str
    level: Optional[str] = ""
    member: Optional[str] = ""
    nested_filters: List["GroupfilterLogic"] = Field(default_factory=list)

    class Config:
        extra = "allow"  # Allow additional attributes


class TableauFilter(BaseModel):
    """Tableau filter structure from JSON."""

    field_name: str
    field_reference: str
    datasource_id: str
    filter_type: TableauFilterType
    filter_class: Optional[TableauFilterClass] = None
    filter_group: Optional[str] = None
    groupfilter_logic: List[GroupfilterLogic] = Field(default_factory=list)
    values: Optional[str] = None
    view_mapping_name: Optional[str] = None

    class Config:
        extra = "allow"


class LookMLFilter(BaseModel):
    """LookML element filter output."""

    field_key: str = Field(..., description="explore.field_name format")
    field_value: str = Field(default="", description="Filter value or expression")
    filter_type: LookMLFilterType = LookMLFilterType.FIELD_FILTER

    @validator("field_key")
    def validate_field_key(cls, v):
        if "." not in v:
            raise ValueError("field_key must be in explore.field format")
        return v


class FilterMappingRule(BaseModel):
    """Rule for mapping Tableau filter to LookML."""

    tableau_type: TableauFilterType
    tableau_class: Optional[TableauFilterClass] = None
    lookml_type: LookMLFilterType
    processor_method: str
    requires_values: bool = True
    description: str = ""


class GroupfilterMappingRule(BaseModel):
    """Rule for processing groupfilter logic."""

    function: GroupfilterFunction
    extract_values: bool
    value_source: Optional[str] = None
    default_value: str = ""
    description: str = ""


class FilterMappingConfig(BaseModel):
    """Complete filter mapping configuration."""

    # Mapping rules
    filter_rules: List[FilterMappingRule] = Field(
        default_factory=lambda: [
            FilterMappingRule(
                tableau_type=TableauFilterType.FILTER_DEFINITION,
                tableau_class=TableauFilterClass.CATEGORICAL,
                lookml_type=LookMLFilterType.FIELD_FILTER,
                processor_method="process_categorical_filter",
                requires_values=True,
                description="Categorical dimension filter",
            ),
            FilterMappingRule(
                tableau_type=TableauFilterType.FILTER_DEFINITION,
                tableau_class=TableauFilterClass.RELATIVE_DATE,
                lookml_type=LookMLFilterType.DATE_FILTER,
                processor_method="process_date_filter",
                requires_values=False,
                description="Date/time filter",
            ),
            FilterMappingRule(
                tableau_type=TableauFilterType.WORKSHEET_CARD,
                lookml_type=LookMLFilterType.FIELD_FILTER,
                processor_method="process_card_filter",
                requires_values=False,
                description="UI filter control",
            ),
        ]
    )

    # Groupfilter rules
    groupfilter_rules: List[GroupfilterMappingRule] = Field(
        default_factory=lambda: [
            GroupfilterMappingRule(
                function=GroupfilterFunction.LEVEL_MEMBERS,
                extract_values=False,
                default_value="",
                description="Include all values",
            ),
            GroupfilterMappingRule(
                function=GroupfilterFunction.MEMBER,
                extract_values=True,
                value_source="member",
                description="Specific value filter",
            ),
            GroupfilterMappingRule(
                function=GroupfilterFunction.UNION,
                extract_values=True,
                value_source="nested_members",
                description="Multiple values filter",
            ),
            GroupfilterMappingRule(
                function=GroupfilterFunction.CROSSJOIN,
                extract_values=False,
                description="Combined dimension filter",
            ),
        ]
    )

    # Field cleaning rules
    field_cleaning: Dict[str, Union[bool, str]] = Field(
        default_factory=lambda: {
            "lowercase": True,
            "replace_spaces": "_",
            "remove_special_chars": True,
            "max_length": 64,
        }
    )

    def get_filter_rule(
        self, tableau_type: str, tableau_class: Optional[str] = None
    ) -> Optional[FilterMappingRule]:
        """Get mapping rule for Tableau filter."""
        for rule in self.filter_rules:
            if rule.tableau_type == tableau_type and (
                tableau_class is None or rule.tableau_class == tableau_class
            ):
                return rule
        return None

    def get_groupfilter_rule(self, function: str) -> Optional[GroupfilterMappingRule]:
        """Get processing rule for groupfilter function."""
        for rule in self.groupfilter_rules:
            if rule.function == function:
                return rule
        return None

    def clean_field_name(self, field_name: str) -> str:
        """Clean field name for LookML compatibility."""
        if not field_name:
            return ""

        cleaned = field_name

        if self.field_cleaning.get("lowercase"):
            cleaned = cleaned.lower()

        if self.field_cleaning.get("replace_spaces"):
            cleaned = cleaned.replace(" ", str(self.field_cleaning["replace_spaces"]))

        if self.field_cleaning.get("remove_special_chars"):
            cleaned = "".join(c for c in cleaned if c.isalnum() or c == "_")

        max_len = self.field_cleaning.get("max_length", 64)
        if len(cleaned) > max_len:
            cleaned = cleaned[:max_len].rstrip("_")

        return cleaned


# Allow forward references for nested models
GroupfilterLogic.model_rebuild()
