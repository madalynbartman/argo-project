from hera.workflows import Steps, Workflow, script


@script()
def echo(message: str):
    print(message)


with Workflow(
    generate_name="single-script-",
    entrypoint="steps",
) as w:
    with Steps(name="steps") as s:
        echo(name="A", arguments={"message": "I'm a step"})
        with s.parallel():
            echo(name="B", arguments={"message": "We're steps"})
            echo(name="C", arguments={"message": "in parallel!"})
        echo(name="D", arguments={"message": "I'm another step!"})

w.create()