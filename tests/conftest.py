
def pytest_addoption(parser):
    parser.addoption(
        "--github-action-run", action="store", default=False, help="Set this to True if it's been called from github action"
    )
