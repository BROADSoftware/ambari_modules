# ambari_modules

This ansible role host a set of modules aimed to ease some operation on Ambari.

* ambari\_configs: Allow to read, define and modify configurations for Hadoop (or others) services managed by Ambari. Doc [at this location](docs/ambari_configs.txt)

* ambari\_service:  Allow to stop/start/restart services managed by Ambari. Doc [at this location](docs/ambari_service.txt)

* ambari\_uri: Usefully replace Ansible uri module for most if not all of Ambari API call. It will handle authentication, automatic cluster id setting and is able to wait completion for a deferred task.[at this location](docs/ambari_uri.txt)

## Requirements

These modules need the python-requests package to be present on the remote node.

## Example Playbook

    # Modify Kafka configuration and restart services 

    - hosts: sr1
      roles:
      - ambari_modules
      tasks:
      - name: Set kafka-broker configuration
        ambari_configs:
          ambari_url: "http://sr1.hdp16:8080"
          username: admin
          password: admin
          operation: set
          type: kafka-broker
          values:
            auto.create.topics.enable: "false"
            log.retention.hours: 220
            
      - name: Restart stale services
        ambari_service:
          ambari_url: "http://sr1.hdp16:8080"
          username: admin
          password: admin
          service: "_stale_"     # '_stale_' is a special token
          state: restarted

## `ambari_server` parameter

If you look at the example above, you will see the first three parameters of each task are always the same. They define how the module acces to Ambari, not what ot does. 
In a larger playbook, this may be cumbersome.

To improve this, theses module accept a parameter `ambari_server` which must be a map hosting one or several of the following keys:

 - `ambari_url`
 - `username`
 - `password`
 - `validate_certs`
 - `ca_bundle_file`

For example, the first task of the above example could be written:

      - name: Set kafka-broker configuration
        ambari_configs:
          ambari_server
            ambari_url: "http://sr1.hdp16:8080"
            username: admin
            password: admin
          operation: set
          type: kafka-broker
          values:
            auto.create.topics.enable: "false"
            log.retention.hours: 220
            
Not a very effective improvment! But we can now rewrite the full sample this way:

    - hosts: sr1
      vars:
        ambariServer:
          ambari_url: "http://sr1.hdp16:8080"
          username: admin
          password: admin
      roles:
      - ambari_modules
      tasks:
      - name: Set kafka-broker configuration
        ambari_configs:
          ambari_server: "{{ambariServer}}"
          operation: set
          type: kafka-broker
          values:
            auto.create.topics.enable: "false"
            log.retention.hours: 220
    
      - name: Restart stale services
        ambari_service:
          ambari_server: "{{ambariServer}}"
          service: "_stale_"
          state: restarted

Obviously, if you have a playbook with a lot of Ambari action, this will improve readability.
          
# License

GNU GPL

Click on the [Link](COPYING) to see the full text.

