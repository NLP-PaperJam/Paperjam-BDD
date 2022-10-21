# Paperjam-BDD

## Installation

```py
conda create -n paperjam-bdd python=3.10
conda activate paperjam-bdd
pip install -r requirements.txt
cp default.env .env
```

default.env is setup for a quick local usage.

## Usage

### update_register

```py
python tools/update_register.py # update all acl_ids
# or
python tools/update_register.py 100 # update only the n first acl_ids (n = 100)
```