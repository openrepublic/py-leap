# `py-leap`

### Python Antelope Framework

`CLEOS` http api & docker based automated end-to-end tests with `pytest`

### Quickstart

Requirements:


- `git`
- `python` >=3.11
- `uv` -> https://docs.astral.sh/uv/

```

git clone https://github.com/openrepublic/py-leap.git

cd py-leap

# setup env
uv venv --python 3.12  # >= 3.11

# install deps
uv sync

# run test
uv run pytest tests/test_libre_dex.py

```
