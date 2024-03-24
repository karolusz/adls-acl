import yamale
from typing import Dict


def config_from_yaml(filename: str) -> Dict:
    """Reads a yaml file into dictionary and validates."""

    config_schema = yamale.make_schema("./schema.yml", parser="PyYAML")
    acls_config = yamale.make_data(filename, parser="PyYAML")

    try:
        yamale.validate(config_schema, acls_config)
    except yamale.YamaleError as e:
        for result in e.results:
            print(f"Error validating data {result.data} with {result.schema}\n\t")
            for error in result.errors:
                print("\t%s" % error)
        raise e

    # yamale puts the dict from yaml into a tuple, into a list
    return acls_config[0][0]
