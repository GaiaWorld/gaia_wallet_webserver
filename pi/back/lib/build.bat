@echo off
if not exist ebin (
    md  ebin
)
erl -make

pause