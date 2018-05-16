@echo off
if not exist .ebin (
    md  .ebin
)
erl -make

:: if this script is executed, pause.
:: if this script called as: ~.bat no-pause, no pause 
@if NOT "%~n1" == "no-pause" (
	@pause  
)