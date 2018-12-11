# ambari_modules

This ansible role host a set of modules aimed to ease some operation on Ambari.

* ambari\_configs: Allow to read, define and modify configurations for Hadoop (or others) services managed by Ambari. Doc [at this location](docs/ambari_configs.txt)

* ambari\_service:  Allow to stop/start/restart services managed by Ambari. Doc [at this location](docs/ambari_service.txt)

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
          
# License

GNU GPL

Click on the [Link](COPYING) to see the full text.

