set cookie=abc
set nodeName=webserver@127.0.0.1
start werl +A 100 +hms 4194304 +hmbs 4194304 -boot start_sasl -config sasl -setcookie %cookie% -name %nodeName% -env ERL_LIBS ../lib

exit