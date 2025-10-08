import dataclasses
from pathlib import Path

from portus.utils import get_today_date_str, read_prompt_template


@dataclasses.dataclass(kw_only=True)
class LighthouseContext:
    """Full context, which provides all necessary information for the agent to answer questions about a specific domain.
    It is designed to help new users to understand what data is needed for precise answers.
    If all fields are filled, the agent is supposed to be able to answer most of the questions.
    It contains both static and dynamic parts excluding tools descriptions.
    It can be easily converted to the system message for LLM.
    """

    template_path: Path
    business_desc: str = ""
    """General description of the business processes, which are behind the data.
    For example, in which cases new records are created or what data corresponds to one operation.
    Paragraphs can be designated by '##' like:
    ## Sales department
       ...
    """
    enum_desc: str = ""
    """Description of special values, which have some context behind them.
    For example, product_type can be 'cloud' or 'on-premise'.
    Sentinel values ('no_data', 'hidden') must be mentioned.
    Each value can have context, e.g. 'cloud' means 'expiration_date' is NULL and correct price is calculated by formula
    Paragraphs can be used with '##' like:
    ## Product types
        'cloud': expiration_date is NULL and correct price is calculated by formula ....
        'on-premise': expiration_date is never NULL and price is in column 'price'
    """
    db_schema: str = ""
    """Description of data schema.
    Can include:
    - Table name
    - Table description
    - Number of rows
    - Column name
    - Column type (including NULL possibility)
    - Column description
    - Column possible values (for low cardinality columns) and top-3 values for others.
    - Column statistics (min, max, mean, std, number of NULLs, etc.)
    For example:
    ## Table `countries` - list of all countries, 100 rows.
        - `country_code` (String): Two letter country code
        - `region_code` (String): Values: CIS, CNS, DACH, EUR
    """
    # sql_metrics: list[InternalMetric] = dataclasses.field(default_factory=list)
    blacklist: list[str] = dataclasses.field(default_factory=list)
    """List of metrics or terms which agent can't calculate. Reason why it's blacklisted can be added.
    For example:
    - 'number of seats' is not available in JetStat
    - 'marketing spending'
    """
    # vocabulary: list[TermDefinition] = dataclasses.field(default_factory=list)
    """List of domain specific terms."""
    personal_info: str = ""
    """User defined information about themselves.
    Can store user preferences, individual definitions, etc."""
    final_instructions: str = ""
    """Most important instructions for the agent"""

    def render(self) -> str:
        prompt_template = read_prompt_template(self.template_path)
        return prompt_template.render(
            date=get_today_date_str(),
            business_desc=self.business_desc,
            enum_desc=self.enum_desc,
            db_schema=self.db_schema,
            # sql_metrics=self.sql_metrics,
            blacklist=self.blacklist,
            # vocabulary=get_vocabulary_list(self.vocabulary),
            personal_info=self.personal_info,
            final_instructions=self.final_instructions,
        ).strip()

    # @classmethod
    # async def build_with_config(
    #     cls,
    #     connection: DuckDBPyConnection,
    #     template_path: str,
    #     business_desc_file: str | None = None,
    #     enum_file: str | None = None,
    #     blacklist_file: str | None = None,
    #     metrics_files: list[str] | None = None,
    #     vocabulary_file: str | dict | None = None,
    #     personal_email: str | None = None,
    #     final_instructions_file: str | None = None,
    #     full_name: str = "",
    # ) -> Self:
    #     db_schema = SimpleDuckDBAgenticExecutor.describe_duckdb_schema(connection)
    #
    #     business_desc = "" if business_desc_file is None else read_resource_file(business_desc_file)
    #
    #     enum_desc = "" if enum_file is None else read_resource_file(enum_file)
    #
    #     if blacklist_file:
    #         with open(blacklist_file, "r") as file:
    #             blacklist = [line for line in file]
    #     else:
    #         blacklist = []
    #
    #     vocab: list[TermDefinition] = []
    #     if isinstance(vocabulary_file, str) and vocabulary_file != "":
    #         vocab_dict = read_vocabulary(Path(vocabulary_file))
    #         vocab = list(vocab_dict.values())
    #     elif isinstance(vocabulary_file, dict):
    #         vocab = list(vocabulary_file.values())
    #
    #     personal_info = get_enriched_personal_info(personal_email, full_name)
    #
    #     metrics = read_internal_metrics(metrics_files)
    #
    #     final_instructions = "" if final_instructions_file is None else read_resource_file(final_instructions_file)
    #
    #     return cls(
    #         template_path=template_path,
    #         business_desc=business_desc,
    #         enum_desc=enum_desc,
    #         db_schema=db_schema,
    #         sql_metrics=list(metrics.values()),
    #         blacklist=blacklist,
    #         vocabulary=vocab,
    #         personal_info=personal_info,
    #         final_instructions=final_instructions,
    #     )
