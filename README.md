# `py-leap`

### Python Antelope Framework

`CLEOS` http api & docker based automated end-to-end tests with `pytest`

### Quickstart

Requirements:


- `git`
- `python` >=3.9
- `poetry` -> https://python-poetry.org/

```

git clone https://github.com/guilledk/py-leap.git -b v0.1a27

cd py-leap

poetry install --with=dev --with=snaps

poetry shell

pytest tests/test_libre_dex.py

```
