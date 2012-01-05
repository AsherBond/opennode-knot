from setuptools import setup, find_packages
from version import get_git_version


setup(
    name = "opennode.oms.knot",
    version = get_git_version(),
    description = """Open Node Virtualization Management Layer""",
    author = "OpenNode Developers",
    author_email = "developers@opennodecloud.com",
    packages = find_packages(),
    namespace_packages = ['opennode'],
    entry_points = {'oms.plugins': ['knot = opennode.knot:KnotPlugin']},
    install_requires = [
        "setuptools", # Redundant but removes a warning
        ],

)
