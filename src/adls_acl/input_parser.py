import yamale
import pkgutil
import logging
from typing import Dict

log = logging.getLogger(__name__)


def config_from_yaml(config_str: str) -> Dict:
    """Reads a yaml file into dictionary and validates."""
    schema = pkgutil.get_data(__name__, "schema.yml").decode("utf-8")
    config_schema = yamale.make_schema(None, parser="PyYAML", content=schema)
    acls_config = yamale.make_data(None, parser="PyYAML", content=config_str)

    try:
        yamale.validate(config_schema, acls_config)
    except yamale.YamaleError as e:
        for result in e.results:
            log.error(result)
        raise e

    # yamale puts the dict from yaml into a tuple, into a list
    return acls_config[0][0]
