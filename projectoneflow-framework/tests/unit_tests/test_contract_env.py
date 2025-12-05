from projectoneflow.framework.contract.env import format_environment_variables


def test_format_environment_variables():
    """
    This test method is used to check environment variables passed to format_environment_variables replace the environment pattern variables with the global variables passed
    """

    # creating temparary varibales with values which has pattern of environment variables patterns "${VARIABLES}"

    temp_variable = {
        "table": "trade",
        "schema": "bronze",
        "catalog": "${CATALOG}",
        "location": "${ROOT_FILE_LOCATION}/${CATALOG}",
        "comment": "${COMMENT}",
    }
    local_variables = {"CATALOG": "dev", "ROOT_FILE_LOCATION": "testlocation"}
    global_variables = {"CATALOG": "prod", "COMMENT": "Testing Purposes"}

    formatted_variable = format_environment_variables(
        source_object=temp_variable,
        local_env=local_variables,
        global_env=global_variables,
    )
    assert (
        formatted_variable["location"]
        == f"{local_variables['ROOT_FILE_LOCATION']}/{local_variables['CATALOG']}"
    )
    assert formatted_variable["catalog"] == f"{local_variables['CATALOG']}"
    assert formatted_variable["comment"] == f"{global_variables['COMMENT']}"
