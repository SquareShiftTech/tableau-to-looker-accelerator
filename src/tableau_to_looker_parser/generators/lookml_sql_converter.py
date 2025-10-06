import re
import logging
import sqlglot
from sqlglot.dialects import DIALECTS
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class LookMLSQLConverter:
    """
    A class for converting LookML SQL between different database dialects
    while preserving LookML-specific placeholders and conditional blocks.
    """

    def __init__(
        self,
        source_dialect: str = "bigquery",
        target_dialect: str = "postgres",
        verbose: bool = False,
    ):
        """
        Initialize the converter with source and target dialects.


        Args:
            source_dialect: The source SQL dialect (e.g., 'bigquery', 'mysql', 'postgres')
            target_dialect: The target SQL dialect (e.g., 'postgres', 'snowflake', 'redshift')
            verbose: Whether to log detailed conversion steps
            logger: Optional logger instance. If None, creates a default logger

        Raises:
            ValueError: If source_dialect or target_dialect is not supported
        """
        self.verbose = verbose
        self._reset_counter()
        self._validate_and_set_dialects(source_dialect, target_dialect)

    def _log(self, message: str, content: str = None, level: str = "debug"):
        """
        Log messages based on the specified level.

        Args:
            message: The message to log
            content: Optional content to log after the message
            level: Logging level ('debug', 'info', 'warning', 'error')
        """
        if self.verbose:
            log_func = getattr(logger, level.lower(), logger.debug)
            log_func(message)
            if content:
                log_func(content)

    def _reset_counter(self):
        """Reset the placeholder counter for each conversion."""
        self._placeholder_counter = 0

    def _validate_dialect(self, dialect: str, dialect_type: str = "dialect") -> bool:
        """
        Validate if a dialect is supported by SQLGlot.

        Args:
            dialect: The dialect name to validate
            dialect_type: Type description for error messages ('source' or 'target')

        Returns:
            True if valid

        Raises:
            ValueError: If dialect is not supported
        """
        supported_dialects = self.get_supported_dialects()
        if dialect.lower() not in supported_dialects:
            error_msg = (
                f"Unsupported {dialect_type} dialect: '{dialect}'. "
                f"Supported dialects are: {', '.join(sorted(supported_dialects))}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        return True

    def _validate_and_set_dialects(self, source_dialect: str, target_dialect: str):
        """
        Validate and set both source and target dialects.

        Args:
            source_dialect: The source dialect to validate and set
            target_dialect: The target dialect to validate and set

        Raises:
            ValueError: If either dialect is not supported
        """
        self._validate_dialect(source_dialect, "source")
        self._validate_dialect(target_dialect, "target")

        # MODULE_BY_DIALECT = {name.lower(): name for name in DIALECTS}
        # self.source_dialect = MODULE_BY_DIALECT[source_dialect.lower()]
        # self.target_dialect = MODULE_BY_DIALECT[target_dialect.lower()]

        self.source_dialect = source_dialect.lower()
        self.target_dialect = target_dialect.lower()

        logger.info(f"Dialects set: {source_dialect} -> {target_dialect}")

    def _preprocess_lookml_sql(self, sql: str) -> Tuple[str, Dict[str, str]]:
        """
        Replace LookML placeholders (${...} and {% ... %}) with safe temporary identifiers.
        Comments out {% ... %} blocks and handles parameters separately.

        Args:
            sql: The original SQL with LookML syntax

        Returns:
            A tuple of (processed_sql, placeholder_mapping)
        """
        if not isinstance(sql, str):
            raise ValueError("SQL must be a string")

        placeholder_map = {}
        self._reset_counter()

        # Helper to generate unique placeholders
        def gen_placeholder(prefix="PH"):
            name = f"__{prefix}_{self._placeholder_counter}__"
            self._placeholder_counter += 1
            return name

        # -----------------------------
        # 1. ${TABLE}.`field` placeholders
        # -----------------------------
        def dollar_table_replacer(match):
            placeholder = match.group(0)
            ph = gen_placeholder()
            placeholder_map[ph] = placeholder.replace("`", '"')
            return ph

        sql = re.sub(r"\$\{TABLE\}.`[^`]+`", dollar_table_replacer, sql)

        # -----------------------------
        # 2. ${field} placeholders
        # -----------------------------
        def dollar_replacer(match):
            placeholder = match.group(0)
            ph = gen_placeholder()
            placeholder_map[ph] = placeholder
            return ph

        sql = re.sub(r"\$\{[A-Za-z0-9_\.]+\}", dollar_replacer, sql)

        # -----------------------------
        # 3. {{ field }} placeholders (Liquid variables)
        # -----------------------------
        def liquid_var_replacer(match):
            placeholder = match.group(0)
            ph = gen_placeholder()
            placeholder_map[ph] = placeholder
            return ph

        # Match anything inside {{ ... }} non-greedily
        sql = re.sub(r"\{\{.*?\}\}", liquid_var_replacer, sql, flags=re.DOTALL)

        # -----------------------------
        # 4. {% raw %} ... {% endraw %} blocks
        # -----------------------------
        def raw_block_replacer(match):
            block = match.group(0)
            ph = gen_placeholder()
            placeholder_map[ph] = block
            return ph

        sql = re.sub(
            r"{%\s*raw\s*%}.*?{%\s*endraw\s*%}",
            raw_block_replacer,
            sql,
            flags=re.DOTALL,
        )

        # -----------------------------
        # Handle {% parameter field %} placeholders
        # -----------------------------
        def param_replacer(match):
            block = match.group(0)
            ph = gen_placeholder()
            placeholder_map[ph] = block
            return ph

        sql = re.sub(r"\{\%\s*parameter\s+[A-Za-z0-9_\.]+\s*\%\}", param_replacer, sql)

        # -----------------------------
        # 6. Comment out remaining {% ... %} blocks
        # -----------------------------
        def comment_liquid_blocks(match):
            block = match.group(0)
            ph = gen_placeholder("COMMENTED_BLOCK")
            placeholder_map[ph] = block
            # Escape any existing comment end markers
            # safe_block = block.replace("*/", "*_/").replace("/*", "/_*")
            return f"/* {ph} */"

        # Match any {% ... %} not already handled
        sql = re.sub(r"\{\%(?!\s*parameter\s)[^%]*\%\}", comment_liquid_blocks, sql)

        return sql, placeholder_map

    def _postprocess_lookml_sql(self, sql: str, placeholder_map: Dict[str, str]) -> str:
        """
        Restore the original LookML placeholders and uncomment {% ... %} blocks
        using the mapping from preprocessing.

        Args:
            sql: The translated SQL with temporary placeholders
            placeholder_map: Mapping from temporary placeholders to original syntax

        Returns:
            SQL with restored LookML syntax
        """
        for safe_name, placeholder in placeholder_map.items():
            if safe_name.startswith("__COMMENTED_BLOCK_"):
                # Remove the comment wrapper and restore original block
                sql = sql.replace(f"/* {safe_name} */", placeholder)
            else:
                # Regular placeholder replacement
                sql = sql.replace(safe_name, placeholder)
        return sql

    def convert(self, sql: str) -> str:
        """
        Convert LookML SQL from one dialect to another, preserving LookML-specific syntax.

        Args:
            sql: The SQL string to convert
            source_dialect: Override the default source dialect for this conversion
            target_dialect: Override the default target dialect for this conversion

        Returns:
            The converted SQL string

        Raises:
            ValueError: If provided dialects are not supported
        """
        # Use provided dialects or fall back to instance defaults
        src = self.source_dialect
        tgt = self.target_dialect

        logger.info(f"Original SQL:\n{sql}\n")

        # Preprocess - replace placeholders and comment out LookML blocks
        try:
            temp_sql, placeholder_map = self._preprocess_lookml_sql(sql)

            self._log("=" * 50)
            self._log(
                "After Preprocessing (commented blocks, replaced placeholders):",
                temp_sql,
            )
            self._log(f"Placeholder mapping: \n\t{placeholder_map}")
            self._log("=" * 50)
        except Exception as e:
            logger.error(f"Error during preprocessing: {e}")
            raise ValueError(f"Error during preprocessing: {e}")

        # Translate using SQLGlot
        try:
            translated_sql = sqlglot.transpile(temp_sql, read=src, write=tgt)[0]
            self._log("After Conversion:", translated_sql)
            self._log("=" * 50)
        except Exception as e:
            logger.error(f"Translation error: {e}")
            raise ValueError(f"Translation error: {e}")

        # Postprocess - restore placeholders and uncomment LookML blocks
        try:
            final_sql = self._postprocess_lookml_sql(translated_sql, placeholder_map)
            self._log("Final Result (after translation and postprocessing):", final_sql)
        except Exception as e:
            logger.error(f"Error during postprocessing: {e}")
            raise ValueError(f"Postprocessing failed: {e}")

        logger.info(f"Converted SQL:\n{sql}\n")
        return final_sql

    def set_dialects(self, source_dialect: str, target_dialect: str):
        """
        Update the default source and target dialects.

        Args:
            source_dialect: The new source SQL dialect
            target_dialect: The new target SQL dialect

        Raises:
            ValueError: If either dialect is not supported
        """
        self._validate_and_set_dialects(source_dialect, target_dialect)

    def set_verbose(self, verbose: bool):
        """
        Enable or disable verbose logging.

        Args:
            verbose: Whether to enable verbose logging
        """
        self.verbose = verbose

    def get_supported_dialects(self) -> list:
        """
        Get a list of supported SQL dialects from SQLGlot.

        Returns:
            List of supported dialect names
        """
        return ["postgres", "bigquery"]
        return list(DIALECTS)


# Example usage and convenience functions
def create_converter(table: dict, verbose: bool = False) -> LookMLSQLConverter:
    """
    Factory function to create a new LookMLSQLConverter instance.

    Args:
        source: Source SQL dialect
        target: Target SQL dialect
        verbose: Whether to enable verbose logging
        logger: Optional logger instance

    Returns:
        A new LookMLSQLConverter instance
    """
    source = "bigquery"
    target = table.get("class", "bigquery")

    if target.lower() == "bigquery":
        return None

    try:
        converter = LookMLSQLConverter(source, target, verbose)
        return converter
    except Exception as e:
        logger.error(f"Failed to create converter: {e}")
        return None


def convert_lookml_sql(
    converter: LookMLSQLConverter, converted_field: dict, is_reference_field=False
) -> str:
    """
    Convenience function for one-off conversions.

    Args:
        sql: The SQL string to convert
        source: Source SQL dialect
        target: Target SQL dialect
        verbose: Whether to log conversion steps
        logger: Optional logger instance

    Returns:
        The converted SQL string
    """
    if is_reference_field:
        try:
            sql = converted_field.get("sql")
            converted_sql = converter.convert(sql)
            converted_field["sql"] = converted_sql
        except Exception as e:
            logger.error(
                f"Conversion failed for {converted_field.get('role')} SQL: {sql}. Error: {e}"
                "TODO: Manual migration required - please convert this formula manually"
            )
            converted_field["sql"] = "'MIGRATION_REQUIRED'"
            converted_field["migration_error"] = True
            converted_field[
                "migration_comment"
            ] = f"""MIGRATION_ERROR: Could not convert calculated field
ORIGINAL_FORMULA: {sql}
CONVERSION_ERROR: {e}
TODO: Manual migration required - please convert this formula manually"""

    if converted_field.get("dimension"):
        try:
            sql = converted_field.get("dimension").get("sql")
            converted_sql = converter.convert(sql)
            converted_field["dimension"]["sql"] = converted_sql
        except Exception as e:
            logger.error(
                f"Conversion failed for dimension SQL: {sql}. Error: {e}"
                "TODO: Manual migration required - please convert this formula manually"
            )
            converted_field["dimension"]["sql"] = "'MIGRATION_REQUIRED'"
            converted_field["dimension"]["migration_error"] = True
            converted_field["dimension"][
                "migration_comment"
            ] = f"""MIGRATION_ERROR: Could not convert calculated field
ORIGINAL_FORMULA: {sql}
CONVERSION_ERROR: {e}
TODO: Manual migration required - please convert this formula manually"""

    if converted_field.get("measure"):
        try:
            sql = converted_field.get("measure").get("sql")
            converted_sql = converter.convert(sql)
            converted_field["measure"]["sql"] = converted_sql
        except Exception as e:
            logger.error(
                f"Conversion failed for measure SQL: {sql}. Error: {e}"
                "TODO: Manual migration required - please convert this formula manually"
            )
            converted_field["measure"]["sql"] = "'MIGRATION_REQUIRED'"
            converted_field["measure"]["migration_error"] = True
            converted_field["measure"][
                "migration_comment"
            ] = f"""MIGRATION_ERROR: Could not convert calculated field
ORIGINAL_FORMULA: {sql}
CONVERSION_ERROR: {e}
TODO: Manual migration required - please convert this formula manually"""

    return converted_field


# Example usage
if __name__ == "__main__":
    # Configure logging for the example
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Example 1: Using the class directly with default logger
    converter = LookMLSQLConverter("bigquery", "postgres", verbose=True)

    sample_sql = """
    SELECT
        ${dimension_name} as dimension,
        {% if event.created_date._in_query %}
            ${event_by_day} as event_data,
        {% endif %}
        COUNT(*) as count
    FROM table_name
    WHERE date >= '2023-01-01'
    """

    result = converter.convert(sample_sql)
    # print(f"Final Result (after translation and postprocessing):\n\n{result}")
