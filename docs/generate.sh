
ansible-doc -M ../library/ ambari_configs 2>/dev/null | sed 's/[(].*ambari_modules[/]library.*[)]//' >ambari_configs.txt
ansible-doc -M ../library/ ambari_host_services 2>/dev/null | sed 's/[(].*ambari_modules[/]library.*[)]//' >ambari_host_services.txt
ansible-doc -M ../library/ ambari_service 2>/dev/null | sed 's/[(].*ambari_modules[/]library.*[)]//' >ambari_service.txt
ansible-doc -M ../library/ ambari_uri 2>/dev/null | sed 's/[(].*ambari_modules[/]library.*[)]//' >ambari_uri.txt
