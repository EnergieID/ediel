#!/usr/bin/env bash
rm dist/*
source venv/bin/activate
python setup.py sdist bdist_wheel
python -m twine upload dist/*