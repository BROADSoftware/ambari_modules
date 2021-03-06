#!/usr/bin/python
# -*- coding: utf-8 -*-

# (c) 2018, BROADSoftware
#
# This software is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software. If not, see <http://www.gnu.org/licenses/>.

DOCUMENTATION = '''
---
module: ambari_configs
short_description: Manage service configuration using Ambari REST API
description:
  - This module will allow you to read, define and modify configurations for Hadoop (or others) services managed by Ambari
author:
  - "Serge ALEXANDRE"
options:
  ambari_url:
    description:
      - The Ambari base URL to access Ambari API. Same host:port as the Ambari Admin GUI. Typically http://myambari.server.com:8080 or https://myambari.server.com:xxxx  
    required: true
    default: None
    aliases: []
  username:
    description:
      - The user name to log on Ambari.
    required: true
    default: None
    aliases: []
  password:
    description:
      - The password associated with the username
    required: true
    default: None
    aliases: []
  validate_certs:
    description:
      - Useful if Ambari connection is using SSL. If no, SSL certificates will not be validated. This should only be used on personally controlled sites using self-signed certificates.
    required: false
    default: True
    aliases: []
  ca_bundle_file:
    description:
      - Useful if Ambari connection is using SSL. Allow to specify a CA_BUNDLE file, a file that contains root and intermediate certificates to validate the Ambari certificate.
      - In its simplest case, it could be a file containing the server certificate in .pem format.
      - This file will be looked up on the remote system, on which this module will be executed. 
    required: false
    default: None
    aliases: []
  ambari_server:
    description:
    - This parameters allow grouping of all Ambari server access related parameters in one dict, this to embed all theses parameters in one variable.
    - Keys of this dict can be C(ambari_url), C(username), C(password), C(validate_certs), C(ca_bundle_file)
    required: false
    default: None
    aliases: []    
  operation:
    description:
      - C(get) Retrieve current configuration values for type 
      - C(set) Set (or remove) some configuration values for type
      - C(list) Retrieve all existing configuration types, with version
    choices:
    - set
    - get
    - list
    required: false
    default: set
    aliases: []
  type:
    description:
      - Configuration type, such as 'kafka-broker', or 'hdfs-site' .
      - Required when (operation != 'list')
    required: false
    default: None
    aliases: []
  values:
    description:
      - A map of of configuration values. See examples below
      - Required when (operation == 'set')
    required: false
    default: None
    aliases: []
  removed:
    description:
    - A list of configuration key which must NOT be present in the configuration
    required: false
    default: None
    type: list
'''

RETURN = '''
types:
  description: Return a dict of configuration type as key and version as unique value
  returned: When (operation == 'list') and success
  type: dict
  sample:  "types": {
            "kafka-broker": {
                "version": 4
            },
            "kafka-env": {
                "version": 2
            },
            "kafka-log4j": {
                "version": 2
            },
            "livy-conf": {
                "version": 2
            },            
            ....
          }

type:
  description: Return the provided configuration type, for reference
  returned: When ((operation == 'get') or (operantion == 'set)) and success
  type: string
  sample: kafka-broker

config: 
  description: Return a dict of configuration value for the provided type
  returned: when (operation == 'get') and success
  type: dict
  sample: "config": {
             "auto.create.topics.enable": "false",
             "auto.leader.rebalance.enable": "true",
             "compression.type": "producer",
             "controlled.shutdown.enable": "true",
             "controlled.shutdown.max.retries": "3",
             "controlled.shutdown.retry.backoff.ms": "5000",
             "controller.message.queue.size": "10",
             ....
          }
'''



EXAMPLES = '''
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
    no_log: true

  # To display current configuration values for a configuration type
  - name: Get kafka-broker configuration
    ambari_configs:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
      operation: get
      type: kafka-broker
    no_log: true
    register: kafkaBrokerConfig

  - debug: var=kafkaBrokerConfig

  # To display all available configuration type
  - name: Get configuration type list (And current versions)
    ambari_configs:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
      operation: list
    no_log: true
    register: typeList
    
  - debug: var=typeList

# Configuration map can also be a variable
- hosts: sr1
  vars:
    brokerConfig:
      auto.create.topics.enable: "false"
      log.retention.hours: 168
  tasks:
  - name: Set another kafka-broker configuration
    ambari_configs:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
      operation: set
      type: kafka-broker
      values: "{{ brokerConfig }}"
    no_log: true  

# Or a plain string (Take care of quotes). 
  - name: Set kafka-broker configuration
    ambari_configs:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
      operation: set
      type: kafka-broker
      values: '{ "log.retention.hours": "220" }'
    no_log: true
'''

import json 
import pprint
import ansible.module_utils.six as six
import warnings
from sets import Set as MySet

HAS_REQUESTS = False

try:
    import requests
    from requests.auth import HTTPBasicAuth
    HAS_REQUESTS = True
except ImportError, AttributeError:
    # AttributeError if __version__ is not present
    pass

# Global, to allow access from error
module = None
logs = []
logLevel = 'None'

pp = pprint.PrettyPrinter(indent=2)

def log(level, message):
    logs.append(level + ':' + str(message))
        
def debug(message):
    if logLevel == 'debug' or logLevel == "info":
        log("DEBUG", message)

def info(message):
    if logLevel == "info" :
        log("INFO", message)

def error(message):
    module.fail_json(msg = message, logs=logs)    

class Parameters:
    pass

CLUSTER_URL="api/v1/clusters"
CONFIGURATION_URL = 'api/v1/clusters/{0}/configurations?type={1}&tag={2}'
DESIRED_CONFIGS_URL = 'api/v1/clusters/{0}?fields=Clusters/desired_configs'
CLUSTER_PUT_URL='api/v1/clusters/{0}'

# JSON Keywords
CLUSTERS = 'Clusters'
ITEMS = 'items'
CLUSTER_NAME="cluster_name"
PROPERTIES = 'properties'
PROPERTIES_ATTRIBUTES = 'properties_attributes'
CLUSTERS = 'Clusters'
DESIRED_CONFIGS = 'desired_configs'
TYPE = 'type'
TAG = 'tag'
ITEMS = 'items'
CLUSTER_NAME="cluster_name"
TAG_PREFIX = 'version'
VERSION="version"

class AmbariConfigApi:
    
    def __init__(self, endpoint, username, password, verify):
        if endpoint.endswith("/"):
            endpoint = endpoint[:-1]
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.verify = verify
        self.auth = HTTPBasicAuth(self.username, self.password)
        warnings.filterwarnings("ignore", ".*Unverified HTTPS.*")
        warnings.filterwarnings("ignore", ".*Certificate has no `subjectAltName`.*")
        result = self.get(CLUSTER_URL)
        if len(result[ITEMS]) != 1:
            error("Invalid response on '{}': {}".format(CLUSTER_URL, result))
        self.cluster = result[ITEMS][0][CLUSTERS][CLUSTER_NAME]
        debug("Cluster name:" + self.cluster)
    
    
    def get(self, path):
        url = self.endpoint + "/" + path
        resp = requests.get(url, auth = self.auth, verify=self.verify, headers={"X-Requested-By":"ANSIBLE"})
        debug("GET " + url + " -> {}".format(resp.status_code)) 
        if resp.status_code == 200:
            result = resp.json()
            return result
        else:
            error("Invalid returned http code '{0}' when calling GET on '{1}'".format(resp.status_code, url))

    def getSafe(self, path):
        url = self.endpoint + "/" + path
        resp = requests.get(url, auth = self.auth, verify=self.verify, headers={"X-Requested-By":"ANSIBLE"})
        debug("GET " + url + " -> {}".format(resp.status_code))
        return resp.status_code, resp.text
         
    def put(self, path, body):
        url = self.endpoint + "/" + path
        #misc.ppprint(body)
        resp = requests.put(url, auth = self.auth, data=json.dumps(body), verify=self.verify, headers={"X-Requested-By":"ANSIBLE"})
        # Following does not works. Explanation may be using json=body set Content-Type in the header to application/json.
        #resp = requests.put(url, auth = self.auth, json=body, verify=self.verify, headers={"X-Requested-By":"ANSIBLE"})
        debug("PUT " + url + " -> ".format(resp.status_code))
        #print(resp.text) 
        #result = resp.json()
        if resp.status_code >= 200 and resp.status_code <= 299:
            return resp.status_code, resp.text
        else:
            error("Invalid returned http code '{0}' when calling PUT on '{1}'  ".format(resp.status_code, url))

    def listConfigTypes(self):
        return self.get(DESIRED_CONFIGS_URL.format(self.cluster))
    
    def getConfigTag(self, type):
        result = self.get(DESIRED_CONFIGS_URL.format(self.cluster))
        if type in result[CLUSTERS][DESIRED_CONFIGS]:
            return result[CLUSTERS][DESIRED_CONFIGS][type][TAG]
        else:
            error("Config type '{}' does not exists!".format(type))
    
    def getConfig(self, type):
        tag = self.getConfigTag(type)
        result = self.get(CONFIGURATION_URL.format(self.cluster, type, tag))
        #misc.ppprint( result)
        if len(result[ITEMS]) != 1:
            error("Invalid response on getConfig(). More than one items: {}".format(result))
        return result[ITEMS][0].get(PROPERTIES, {}), result[ITEMS][0].get(PROPERTIES_ATTRIBUTES, {})
    
    def changeConfig(self, configType, newProperties, removed, checkMode):
        properties, attributes = self.getConfig(configType)
        changed = False
        for key in newProperties:
            value = str(newProperties[key])
            if key in properties and properties[key] == value:
                debug("{}/{}/{} Value unchanged".format(configType, key, value))
            else:
                if not key in properties:
                    debug("{}/{}/{}: Value created".format(configType, key, value))
                else: 
                    debug("{}/{}/{}=>{}: Value changed".format(configType, key, properties[key], value))
                properties[key] = value
                changed = True
        if not removed == None:
            toRemove = MySet(removed)   # To optimize lookup
            keys = properties.keys()
            for key in keys:
                if key in toRemove:
                    changed = True
                    del properties[key]
        if changed and not checkMode:
            newTag = TAG_PREFIX + str(int(time.time() * 1000000))
            newConfig = {
                CLUSTERS: {
                    DESIRED_CONFIGS: {
                        TYPE: configType,
                        TAG: newTag,
                        PROPERTIES: properties
                    }
                }
            }
            if len(attributes.keys()) > 0:
                newConfig[CLUSTERS][DESIRED_CONFIGS][PROPERTIES_ATTRIBUTES] = attributes            
            self.put(CLUSTER_PUT_URL.format(self.cluster), newConfig)
        return changed
    
GET="get"
SET="set"
LIST="list"


def param2(token, moduleParams, ambariServer, default, required):
    if token in moduleParams and moduleParams[token] != None:
        return moduleParams[token]
    elif ambariServer != None and token in ambariServer and ambariServer[token] != None:
        return ambariServer[token]
    elif default != None:
        return default
    elif required:
        error("Missing required attribute: '{}'".format(token))
    else:
        return None

def main():
    global module
    module = AnsibleModule(
        argument_spec = dict(
            ambari_server = dict(type='raw', required=False),
            ambari_url = dict(type='str', required=False),
            username = dict(required=False, type='str'),
            password = dict(required=False, type='str'),
            validate_certs = dict(required=False, type='bool'),
            ca_bundle_file = dict(required=False, type='str'),
            operation = dict(type='str', required=False, choices=[GET, SET, LIST], default=SET),
            type = dict(required=False, type='str'),
            values = dict(type='raw', required=False),
            removed = dict(type='list', required=False),
            log_level = dict(type='str', required=False, default="None")
        ),
        supports_check_mode=True
    )
    if not HAS_REQUESTS:
        error(msg="python-requests package is not installed")    

    ambariServer = module.params["ambari_server"]
    if ambariServer != None:   
        if not isinstance(ambariServer, six.string_types):
            ambariServer = json.dumps(ambariServer)
        ambariServer = json.loads(ambariServer)

    p = Parameters()
    p.ambariUrl = param2('ambari_url', module.params, ambariServer, None, True)
    p.username = param2('username', module.params, ambariServer, None, True)
    p.password = param2('password', module.params, ambariServer, None, True)
    p.validateCerts = param2('validate_certs', module.params, ambariServer, True, True)
    p.ca_bundleFile = param2('ca_bundle_file', module.params, ambariServer, None, False)
    p.operation = module.params["operation"]
    p.type = module.params["type"]
    p.values = module.params["values"]
    p.removed = module.params["removed"]
    p.logLevel = module.params['log_level']
    p.checkMode = module.check_mode
    
    global  logLevel
    logLevel = p.logLevel
    
    if p.ca_bundleFile != None:
        verify = p.ca_bundleFile
    else:
        verify = p.validateCerts
    
    if p.operation != LIST and p.type == None:
        error("'type' is mandatory when operation != 'list'")
    
    if p.operation == SET and p.values == None and p.removed == None:
        error("One of 'values' or 'removed' is mandatory when operation == 'set'")

    api = AmbariConfigApi(p.ambariUrl, p.username, p.password, verify)
    if p.operation == LIST:
        result = api.listConfigTypes()
        types = {}
        for k,v in result[CLUSTERS][DESIRED_CONFIGS].iteritems():
            types[k] = { "version": v[VERSION] }
        module.exit_json(changed=False, types=types, logs=logs)
    elif p.operation == GET:
        result = api.getConfig(p.type)
        module.exit_json(changed=False, type=p.type, config=result, logs=logs)
    elif p.operation == SET:
        if p.values == None: # This is the case we have only 'removed'
            values = {} 
        else:
            if not isinstance(p.values, six.string_types):
                p.values = json.dumps(p.values)
            values = json.loads(p.values)
        changed = api.changeConfig(p.type, values, p.removed, p.checkMode)
        module.exit_json(changed=changed, type=p.type, logs=logs)
    else:
        error("Unimplemented operation '{}'".format(p.operation))

from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()

