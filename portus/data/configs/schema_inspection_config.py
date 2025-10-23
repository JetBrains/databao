from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ValueSamplingStrategy(StrEnum):
    """How to sample values from columns."""

    CATEGORICAL_ONLY = "categorical_only"
    """Only sample values from categorical columns."""
    TOP_P = "top_p"
    """Sample top_p values (analogous to nucleus sampling) from all columns."""
    NONE = "none"
    """Do not sample values from any columns."""


class InspectionOptions(BaseModel):
    inspect_column_stats: bool = True
    """Whether to use value stats (e.g. min, max, unique counts) in the summary."""

    value_sampling_strategy: ValueSamplingStrategy = ValueSamplingStrategy.CATEGORICAL_ONLY
    """How to proceed with samples from columns"""

    max_enum_values: int = 10
    """Maximum number of values to sample for enum (low cardinality) columns."""

    max_unique_rate: float = 0.3
    """
    Maximum uniqueness rate - i.e. count(distinct)/count(total), for a column to be have a sample of its
    values sampled in the column summary
    """

    cache_intermediate_results: bool = False
    """Whether to cache intermediate schema inspection results."""

    tables_regex: str | None = None
    """Regex to include **additional** tables in the schema inspection. 
    Tables listed in the semantic dict are always included.
    
    Mainly for generated dbt projects, where not all tables are documented and would otherwise get skipped.
    """
    columns_regex: str | None = None
    """Regex to include **additional** columns in the schema inspection. 
    Columns listed in the semantic dict are always included."""

    retrieval_set_limit: int | None = Field(default=None, ge=0)
    """The size of a limited subset of the contents of a table/view to retrieve first and then perform 
    aggregations on. The column stats metrics may not be fully accurate, 
    but the time will be noticeably reduced."""

    model_config = ConfigDict(extra="forbid")

    def model_dump_for_cache(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"cache_intermediate_results"})


class SchemaSummaryType(StrEnum):
    FULL = "full"
    """
    Recursively inspect all tables in the database and provide summaries of each table containing summaries
    of all constituent columns.
    """
    LIST_ALL_TABLES = "list_all_tables"
    """
    Only provide a list of the table names and (optionally (default)) short table descriptions 
    available in the database. No column summaries are included.
    """
    COMPACT = "compact"
    """List all tables where each table is summarized in a single line with column names and types."""
    # TODO hybrid - include top k tables (ranked by an LLM during preprocessing), list the rest
    # TODO table linking for each question


class SchemaInspectionConfig(BaseModel):
    summary_type: SchemaSummaryType = SchemaSummaryType.FULL

    inspection_options: InspectionOptions = Field(default_factory=InspectionOptions)

    cache_final_results: bool = True
    """Whether to cache the final schema inspection results."""

    force_update_final_results_cache: bool = False
    """Whether to force an update of the cache of the final results. Ignored if cache_final_results is false."""

    model_config = ConfigDict(extra="forbid")
