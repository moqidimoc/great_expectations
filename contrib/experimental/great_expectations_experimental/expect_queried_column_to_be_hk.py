"""
This is a template for creating custom QueryExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import Any, Dict, Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


# This class defines the Expectation itself
# <snippet>
class ExpectQueriedColumnToBeHK(QueryExpectation):
    # </snippet>
    # <snippet>
    """Tests whether every row from a column containing a hash key has 32 characters/bytes"""
    # </snippet>

    # This is the id string of the Metric(s) used by this Expectation.
    # <snippet>
    metric_dependencies = ("query.column",)
    # </snippet>

    # This is the default, baked-in SQL Query for this QueryExpectation
    # <snippet>
    query = """
            SELECT {col}, 
            CAST(COUNT( {col} ) AS float) / (SELECT CAST(COUNT( {col} ) AS float) FROM {active_batch})
            FROM {active_batch}
            WHERE LENGTH( {col} ) = {value}
            """
    # </snippet>

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # <snippet>
    success_keys = (
        "column",
        "value",
        "threshold",
        "query",
    )
    # </snippet>

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "column": None,
        "value": 32,
        "threshold": 1,
        "query": query,  # Passing the above `query` attribute here as a default kwarg allows for the Expectation to be run with the defaul query, or have that query overridden by passing a `query` kwarg into the expectation
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """
        super().validate_configuration(configuration)
        value = configuration["kwargs"].get("value")
        threshold = configuration["kwargs"].get("threshold")
        
        if configuration is None:
            configuration = self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        try:
            assert value is not int, "'value' must be an integer"
            assert (isinstance(threshold, (int, float)) and 0 < threshold <= 1) or (
                isinstance(threshold, list)
                and all(isinstance(x, (int, float)) for x in threshold)
                and all([0 < x <= 1 for x in threshold])
                and 0 < sum(threshold) <= 1
            ), "'threshold' must be 1, a float between 0 and 1, or a list of floats whose sum is between 0 and 1"
            if isinstance(threshold, list):
                assert isinstance(value, list) and len(value) == len(
                    threshold
                ), "'value' and 'threshold' must contain the same number of arguments"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    # <snippet>
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        
        value = configuration["kwargs"].get("value")
        threshold = configuration["kwargs"].get("threshold")
        query_result = metrics.get("query.column")
        query_result = dict(query_result)
        
        if isinstance(value, list):
            success = all(
                query_result[value[i]] >= threshold[i] for i in range(len(value))
            )
            
            return {
                "success": success,
                "result": {
                    "observed_value": [
                        query_result[value[i]] for i in range(len(value))
                    ]
                },
            }
        success = query_result[value] >= threshold
        
        return {
            "success": success,
            "result": {"observed_value": query_result[value]},
        }
        
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # <snippet>
    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "correct_col": ["A2D0F09F80A7A262817B8D616CFA096D", "1D81827C009F0CABE81437Y9F1133986", "6977AC0FA73B36A46B1B6F5926015667", "A01A51CCC511D707DHCE88DCFBCA351F", "08244CA11372E3A581902A06DF7C64C9"],
                        "incorrect_col": ["08244CA11372E3A581902A", "1D81827C009F0CABE81437Y9F1133986KAP47F", "A2D0F09F80A7A262817B8D616CFA096D", "A7A62817A2D0F09F806096D", "36A46B1B6F27C001902A06DF7C69F0CAB5CE88DCFBCA35926"],
                    },
                },
            ],
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "correct_col",
                        "value": 32,
                        "threshold": 0.9,
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "incorrect_col",
                        "value": 32,
                        "threshold": 0.9,
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
            ]
        }
    ]
    # </snippet>

    # This dictionary contains metadata for display in the public gallery
    # <snippet>
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }
    # </snippet


if __name__ == "__main__":
    # <snippet>
    ExpectQueriedColumnToBeHK().print_diagnostic_checklist()
    # </snippet
