from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from portus.utils import read_prompt_template

_VALUE_STATS_SUMMARY_TEMPLATE = read_prompt_template(Path("column_value_stats_summary.jinja"))


class TopKValuesElement(BaseModel):
    value: Any
    count: int  # to be able to filter out based on min number of occurrences
    frequency: float


class ColumnValuesStats(BaseModel):
    # general data stats
    n_total: int | None = None
    n_unique: int | None = None
    unique_rate: float | None = None
    """
    Measures the rate of unique values in the column - helps decide how to display the number of unique
    items - if we have 1M unique items out of 1M items in total, this info will not be particularly helpful inside the 
    context, instead we could say either "column contains unique values", or if we don't know about any uniqueness 
    constrains: "no value is duplicated inside the column" (i.e. omitting the usage of the word `unique` which may 
    convey false notion of presence of a uniqueness constraint). 
    """
    null_rate: float | None = None

    # numeric stats
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    std: float | None = None

    # formal str stats
    min_str_len: int | None = None
    max_str_len: int | None = None
    mean_str_len: float | None = None
    min_token_count: int | None = None  # if strings contain punctuation / whitespace - number of tokens by whitespace
    max_token_count: int | None = None
    mean_token_count: float | None = None
    all_lowercase_rate: float | None = None  # display in context only if 100%
    all_uppercase_rate: float | None = None
    contains_numbers: bool | None = None
    contains_punctuation: bool | None = None
    contains_whitespace: bool | None = None

    # TODO: find an elegant way to get the common prefixes - also define what common prefixes actually means
    common_prefixes: float | None = None

    # samples
    top_k_values_with_frequencies: list[TopKValuesElement] | None = None
    """
    A list holding the top k (k=50) most frequent values and their frequencies.
    """

    def is_empty(self) -> bool:
        """
        Checks if the values of all fields are None or empty lists.
        """
        props: dict[str, Any] = self.model_json_schema()["properties"]
        for field in props:
            field_value = getattr(self, field)
            if field_value is not None or (isinstance(field_value, list) and len(field_value) > 0):
                return False
        return True

    def get_top_p_sample_values(
        self, top_p: float = 0.3, min_sample_values: float = 5, max_sample_values: float = 5
    ) -> list[Any] | None:
        """
        Analogous to nucleus sampling get the smallest subset of values, whose cumulative frequency is greater than or
        equal to p. The number of sampled values cannot be less than min_sample_values and cannot be more than
        the max_sample_values.
        """
        if self.top_k_values_with_frequencies is None:
            return None

        chosen_values: list[Any] = []
        p = 0.0
        for element in self.top_k_values_with_frequencies:
            p += element.frequency
            if (p >= top_p and len(chosen_values) >= min_sample_values) or len(chosen_values) >= max_sample_values:
                break
            chosen_values.append(element.value)

        return chosen_values

    def summarize(self) -> str:
        dict_representation = self.model_dump()
        dict_representation.pop("top_k_values_with_frequencies")
        dict_representation["sample_values"] = self.get_top_p_sample_values()
        return _VALUE_STATS_SUMMARY_TEMPLATE.render(**dict_representation, lstrip_blocks=True, trim_blocks=True)


class ColumnSchema(BaseModel):
    name: str
    dtype: str
    description: str | None = None
    values: list[str] = Field(default_factory=list)
    value_stats: ColumnValuesStats = ColumnValuesStats()

    model_config = ConfigDict(extra="forbid")


class TableSchema(BaseModel):
    name: str
    schema_name: str | None = None
    description: str | None = None
    columns: dict[str, ColumnSchema]

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.name}" if self.schema_name else self.name

    model_config = ConfigDict(extra="forbid")


class DatabaseSchema(BaseModel):
    db_type: str
    name: str | None = None
    description: str | None = None
    tables: dict[str, TableSchema] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")
