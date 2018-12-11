
ansible-doc -M ../library/ ambari_configs 2>/dev/null | sed 's/[(].*ambari_modules[/]library.*[)]//' >ambari_configs.txt
