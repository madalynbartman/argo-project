from hera.workflows import Workflow, Container
from hera.shared import global_config
from hera.auth import ArgoCLITokenGenerator

global_config.host = "http://localhost:2746"
global_config.token = ArgoCLITokenGenerator

with Workflow(generate_name="local-test-", entrypoint="c") as w:
    Container(name="c", image="docker/whalesay", command=["cowsay", "hello"])

w.create()