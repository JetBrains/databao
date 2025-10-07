import logging
from types import NoneType
from typing import Any, TypeVar, get_args

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from portus._template_utils import read_package_template

_LOGGER = logging.getLogger(__name__)

_VALUE_STATS_SUMMARY_TEMPLATE = read_package_template("portus.data_source", "column_value_stats_summary.jinja")

T = TypeVar("T")
R = TypeVar("R")


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

    # TODO: curtail coercion scope - currently it might be hiding too many errors
    @field_validator("*", mode="before")
    @classmethod
    def coerce_invalid_to_none(cls, v: T, info: ValidationInfo) -> T | None:
        """
        A failsafe mechanism to coerce invalid values to None. This is mainly included because of sqlite as it is a
        dynamically typed
        """

        def _try_coercing(v: T, types: tuple[type[R], ...]) -> R:
            if v is None:
                if NoneType in types:
                    return None  # type: ignore[return-value]
                else:
                    raise ValueError(f"Cannot coerce None into one of {types}")
            for type_ in types:
                try:
                    return type_(v)  # type: ignore[call-arg]
                except (ValueError, TypeError):
                    pass
            raise ValueError(f"Cannot coerce {v} into one of {types}")

        field_type = cls.model_fields[str(info.field_name)].annotation
        try:
            return _try_coercing(v, get_args(field_type))  # type: ignore[no-any-return]
        except ValueError:
            pass
        if NoneType in get_args(field_type):
            _LOGGER.warning(f"Coercing invalid value {v} to None for field {info.field_name}")
            return None
        raise ValueError(
            f"Cannot coerce {info.field_name} into None from {type(v)} as the expected types do not include NoneType,"
        )

    def is_empty(self) -> bool:
        """
        Checks if the values of all fields are None or empty lists.
        """
        for field in self.model_json_schema()["properties"]:
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
    schema_name: str | None = None  # Not all databases have the concept of a schema
    description: str | None = None
    columns: dict[str, ColumnSchema]

    model_config = ConfigDict(extra="forbid")

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.name}" if self.schema_name else self.name


class DatabaseSchema(BaseModel):
    db_type: str
    name: str | None = None
    description: str | None = None
    tables: dict[str, TableSchema] = Field(default_factory=dict)  # TODO list instead of dict to avoid key confusion

    model_config = ConfigDict(extra="forbid")
