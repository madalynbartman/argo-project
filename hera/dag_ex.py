from hera.workflows import (
    DAG,
    Workflow,
    script,
)


# Notice that we are using the script decorator to define the function.
# This is required in order to use the function as a template.
# The decorator also allows us to define the image that will be used to run the function and
# other parameters that are specific to the Script template type.
@script(add_cwd_to_sys_path=False, image="python:alpine3.6")
def say(message):
    print(message)


with Workflow(generate_name="dag-diamond-", entrypoint="diamond") as w:
    # Note that we need to explicitly specify the DAG template type.
    with DAG(name="diamond"):
        # We can now use the decorated function as tasks in the DAG.
        A = say(name="A", arguments={"message": "A"})
        B = say(name="B", arguments={"message": "B"})
        C = say(name="C", arguments={"message": "C"})
        D = say(name="D", arguments={"message": "D"})
        # We can also use the `>>` operator to define dependencies between tasks.
        A >> [B, C] >> D
