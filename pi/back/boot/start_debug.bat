@echo off
@echo �������ؿ����ڵ�
::msg %username% /time:3 /w "�����������ؿ����ڵ�..."
goto continue
:continue
start werl -boot start_sasl -config sasl -env ERL_LIBS ../lib -env ERL_PLUGIN_START_MOD debug

exit