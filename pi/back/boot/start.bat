set cookie=abc
set nodeName=webserver@127.0.0.1
start werl -boot start_sasl -config sasl -setcookie %cookie% -name %nodeName% -env ERL_LIBS ../lib

exit