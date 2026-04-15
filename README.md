# CoSy-Luigi

<div align="center">

<img src="https://raw.githubusercontent.com/cls-python/cosy-luigi/main/docs/assets/images/logo.svg" alt="cosy-luigi logo" width="400" role="img">

|          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Package  | [![PyPI - Version](https://img.shields.io/pypi/v/cosy-luigi.svg?logo=pypi&label=&labelColor=grey&logoColor=gold&style=flat-square)](https://pypi.org/project/cosy-luigi) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cosy-luigi.svg?logo=python&label=&labelColor=grey&logoColor=gold&style=flat-square)](https://pypi.org/project/cosy-luigi)                                                                                                                                                                                         |
| License  | [![License](https://img.shields.io/github/license/cls-python/cosy-luigi?color=9E2165&logo=apache&label=&labelColor=grey&style=flat-square)](https://opensource.org/licenses/Apache-2.0)                                                                                                                                                                                                                                                                                                                                                                     |
| Package  | [![CI - Test](https://img.shields.io/github/actions/workflow/status/cls-python/cosy-luigi/checks.yml?logo=pytest&label=checks&style=flat-square)](https://github.com/cls-python/cosy-luigi/actions/workflows/checks.yml) [![CD - Release cosy-luigi](https://img.shields.io/github/actions/workflow/status/cls-python/cosy-luigi/release.yml?logo=pytest&label=release&style=flat-square)](https://github.com/cls-python/cosy-luigi/actions/workflows/release.yml)                                                                                       |
| Docs     | [![Docs - Release](https://img.shields.io/github/actions/workflow/status/cls-python/cosy-luigi/check-docs.yml?logo=materialformkdocs&logoColor=white&label=checks&style=flat-square)](https://github.com/cls-python/cosy-luigi/actions/workflows/check-docs.yml) [![Docs - Checks](https://img.shields.io/github/actions/workflow/status/cls-python/cosy-luigi/deploy-docs.yml?logo=materialformkdocs&logoColor=white&label=deploy&style=flat-square)](https://github.com/cls-python/cosy-luigi/actions/workflows/deploy-docs.yml)                          |
| Coverage | [![codecov](https://img.shields.io/codecov/c/github/cls-python/cosy-luigi/main?token=5FR1DKS09I&logo=codecov&label=main&labelColor=grey&style=flat-square)](https://codecov.io/github/cls-python/cosy-luigi/tree/main) [![codecov](https://img.shields.io/codecov/c/github/cls-python/cosy-luigi/develop?token=5FR1DKS09I&logo=codecov&label=develop&labelColor=grey&style=flat-square)](https://codecov.io/github/cls-python/cosy-luigi/tree/develop)                                                                                                      |
| Traits   | [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg?style=flat-square)](https://hatch.pypa.io/latest/) [![Checked with mypy](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fcls-python%2Fcosy-luigi%2Fmain%2Fdocs%2Fassets%2Fbadges%2Fmypy.json&style=flat-square)](http://mypy-lang.org/) [![Checked with Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&color=4051b5&style=flat-square)](https://github.com/astral-sh/ruff) |

</div>

-----
`CoSy-Luigi` is a highly efficient open-source pipeline synthesis framework. 
It enables modeling structural variance in pipelines using established object-oriented programming (OOP) paradigms, 
such as inheritance, resulting in a low barrier to entry.

## Examples




## Installation

```console
pip install --pre cosy-luigi
```

Since `cosy-luigi` is in pre-release state, `PyPi` distributions are likely to be outdated.

To work around that, you can stay updated with a nightly build:  

```console
pip install https://github.com/cls-python/cosy-luigi/releases/download/nightly/cosy_luigi-nightly.tar.gz
```

## Documentation
Please head over to the [documentation](https://cls-python.github.io/cosy-luigi/) to [get started](https://cls-python.github.io/cosy-luigi/quick-start/). 

## Contributing
Please contribute via a fork if not part of the cls-python org, and contribute via a `feature/` or `bugfix/` branch if you are part of the org. 

Before making a PR:
- For code, please run `hatch fmt` and `hatch run types:check` to make sure you meet code quality standards. 
- For docs, please run `hatch run docs:check` to make sure that everything is in order. 

These are run as part of the PR, so if these do not pass for you locally, the PR is guaranteed not to be mergeable. 

 
## License

`cosy-luigi` is distributed under the terms of the [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html) license.

