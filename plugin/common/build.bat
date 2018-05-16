@echo off
if not exist .ebin (
    md  .ebin
)
for /R "./" %%s in (*.erl) do ( 
	echo %%s
	erlc +debug_info -o ".ebin" -v "%%s"

) 

pause