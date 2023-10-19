# `py-leap`

### Python Antelope Framework

`CLEOS` http api & docker based automated end-to-end tests with `pytest`

### Quickstart

Requirements:


- `git`
- `python` >=3.8
- `poetry` -> https://python-poetry.org/
- `docker` (Optional: required for testing framework)

```

git clone https://github.com/guilledk/py-leap.git

cd py-leap

poetry install --with=nodemngr --with=dev

poetry shell

pytest

```
