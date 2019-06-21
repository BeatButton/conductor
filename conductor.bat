@echo off

net use O: \\SERVER\Office$

call venv\scripts\activate.bat

python -m conductor
