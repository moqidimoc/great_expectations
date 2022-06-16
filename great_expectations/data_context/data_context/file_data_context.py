import configparser
import logging
import os
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.data_context_variables import (
    FileDataContextVariables,
)

logger = logging.getLogger(__name__)


class FileDataContext(AbstractDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        self.runtime_environment = runtime_environment or {}
        self._context_root_dir = context_root_dir
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )

    def _init_variables(self) -> FileDataContextVariables:
        raise NotImplementedError

    def _check_global_usage_statistics_opt_out(self) -> bool:
        return self._check_global_usage_statistics_env_var_and_file_opt_out()

    @staticmethod
    def _check_global_usage_statistics_env_var_and_file_opt_out() -> bool:
        if os.environ.get("GE_USAGE_STATS", False):
            ge_usage_stats = os.environ.get("GE_USAGE_STATS")
            if ge_usage_stats in AbstractDataContext.FALSEY_STRINGS:
                return True
            else:
                logger.warning(
                    "GE_USAGE_STATS environment variable must be one of: {}".format(
                        AbstractDataContext.FALSEY_STRINGS
                    )
                )
        for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
            config = configparser.ConfigParser()
            states = config.BOOLEAN_STATES
            for falsey_string in AbstractDataContext.FALSEY_STRINGS:
                states[falsey_string] = False
            states["TRUE"] = True
            states["True"] = True
            config.BOOLEAN_STATES = states
            config.read(config_path)
            try:
                if config.getboolean("anonymous_usage_statistics", "enabled") is False:
                    # If stats are disabled, then opt out is true
                    return True
            except (ValueError, configparser.Error):
                pass
        return False

    @classmethod
    def _get_global_config_value(
        cls,
        environment_variable: str,
        conf_file_section: Optional[str] = None,
        conf_file_option: Optional[str] = None,
    ) -> Optional[str]:
        assert (conf_file_section and conf_file_option) or (
            not conf_file_section and not conf_file_option
        ), "Must pass both 'conf_file_section' and 'conf_file_option' or neither."
        if environment_variable and os.environ.get(environment_variable, False):
            return os.environ.get(environment_variable)
        if conf_file_section and conf_file_option:
            for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
                config = configparser.ConfigParser()
                config.read(config_path)
                config_value = config.get(
                    conf_file_section, conf_file_option, fallback=None
                )
                if config_value:
                    return config_value
        return None

    def _get_data_context_id_override(self) -> Optional[str]:
        return self._get_data_context_id_override_from_env_var_and_file()

    def _get_data_context_id_override_from_env_var_and_file(self) -> Optional[str]:
        return self._get_global_config_value(
            environment_variable="GE_DATA_CONTEXT_ID",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="data_context_id",
        )

    def _get_usage_stats_url_override(self) -> Optional[str]:
        return self._get_config_value_from_env_var_and_file()

    def _get_config_value_from_env_var_and_file(self) -> Optional[str]:
        return self._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )

        # def _apply_global_config_overrides(

    #     self, config: DataContextConfig
    # ) -> DataContextConfig:
    #     # check for global usage_statistics opt out
    #     validation_errors: dict = {}
    #     config_with_global_config_overrides: DataContextConfig = copy.deepcopy(config)
    #
    #     if self._check_global_usage_statistics_opt_out():
    #         logger.info(
    #             "Usage statistics is disabled globally. Applying override to project_config."
    #         )
    #         config_with_global_config_overrides.anonymous_usage_statistics.enabled = (
    #             False
    #         )
    #
    #     # check for global data_context_id
    #     global_data_context_id = self._get_global_config_value(
    #         environment_variable="GE_DATA_CONTEXT_ID",
    #         conf_file_section="anonymous_usage_statistics",
    #         conf_file_option="data_context_id",
    #     )
    #     if global_data_context_id:
    #         data_context_id_errors = anonymizedUsageStatisticsSchema.validate(
    #             {"data_context_id": global_data_context_id}
    #         )
    #         if not data_context_id_errors:
    #             logger.info(
    #                 "data_context_id is defined globally. Applying override to project_config."
    #             )
    #             config_with_global_config_overrides.anonymous_usage_statistics.data_context_id = (
    #                 global_data_context_id
    #             )
    #         else:
    #             validation_errors.update(data_context_id_errors)
    #     # check for global usage_statistics url
    #     global_usage_statistics_url = self._get_global_config_value(
    #         environment_variable="GE_USAGE_STATISTICS_URL",
    #         conf_file_section="anonymous_usage_statistics",
    #         conf_file_option="usage_statistics_url",
    #     )
    #     if global_usage_statistics_url:
    #         usage_statistics_url_errors = anonymizedUsageStatisticsSchema.validate(
    #             {"usage_statistics_url": global_usage_statistics_url}
    #         )
    #         if not usage_statistics_url_errors:
    #             logger.info(
    #                 "usage_statistics_url is defined globally. Applying override to project_config."
    #             )
    #             config_with_global_config_overrides.anonymous_usage_statistics.usage_statistics_url = (
    #                 global_usage_statistics_url
    #             )
    #         else:
    #             validation_errors.update(usage_statistics_url_errors)
    #     if validation_errors:
    #         logger.warning(
    #             "The following globally-defined config variables failed validation:\n{}\n\n"
    #             "Please fix the variables if you would like to apply global values to project_config.".format(
    #                 json.dumps(validation_errors, indent=2)
    #             )
    #         )
    #     return config_with_global_config_overrides
