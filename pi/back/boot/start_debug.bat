@echo off
@echo 启动本地开发节点
::msg %username% /time:3 /w "正在启动本地开发节点..."
goto continue
:continue
start werl -boot start_sasl -config sasl -env ERL_LIBS ../lib -env ERL_PLUGIN_START_MOD debug

exit