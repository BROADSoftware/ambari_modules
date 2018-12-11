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
module: ambari_service
short_description: Manage service lifecycle using Ambari REST API
description:
  - This module will allow you to stop/start/restart services managed by Ambari
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
  service:
    description:
    - The service we want to act on. such as 'HDFS', 'KAFKA_BROKER',...
    - Required when (state != 'list')
    - Can take special value '_stale_' to restart all stale services (Typically after a configuration modification)
    required: false
    default: None
    aliases: []
  components:
    description:
    - A list of service component to restart. 
    - Valid only when (state == 'restarted') and (service != '_stale_') 
    required: false
    default: All components of the given services
    aliases: []
  state:
    description:
    - Target state for the provided service
    - C(list) is a special case, allowing to retrieve all available services and associated components of the cluster. 
    choices:
    - stopped
    - started
    - restarted
    - list
    required: true
    default: None
    aliases: []
  wait:
    description: Should the module wait for the operation to be completed.
    type: bool
    required: false
    default: True
'''

RETURN = '''
services:
  description: A dict of all services as key and list of associated components as value
  returned: When (state == 'list') and success
  type: dict
  sample: "services": {
            "AMBARI_METRICS": {
                "components": [
                    "METRICS_COLLECTOR",
                    "METRICS_GRAFANA",
                    "METRICS_MONITOR"
                ]
            },
            "HBASE": {
                "components": [
                    "HBASE_CLIENT",
                    "HBASE_MASTER",
                    "HBASE_REGIONSERVER"
                ]
            },
            "HDFS": {
                "components": [
                    "DATANODE",
                    "HDFS_CLIENT",
                    "JOURNALNODE",
                    "NAMENODE",
                    "ZKFC"
                ]
            },q
            ...
        }

task:
  description: The task id of the performed operation.
  returned: when (state == 'stop' or state == 'start' or (state == 'restarted' and service == '_stale_'))
  type: string
  sample: 34

tasks:
  description: A list of task id of the performed operation.
  returned: when (state == 'restarted' and service != '_stale_')
  type: list of string
  sample: [36, 37, 38]
'''

EXAMPLES = '''
  - name: Restart stale services
    ambari_service:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
      service: "_stale_"
      state: restarted
    no_log: true
  
  - name: Restart HBASE
    ambari_service:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
      service: "HBASE"
      state: restarted
    no_log: true
  
  - name: Restart some components of HBASE
    ambari_service:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
      service: "HBASE"
      components:
      - HBASE_MASTER
      - HBASE_REGIONSERVER
      state: restarted
    no_log: true
  
  - name: Stop HBASE service
    ambari_service:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
      service: "HBASE"
      state: stopped
    no_log: true
'''

import json 
import pprint
import ansible.module_utils.six as six
import warnings
import time
import json
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
    logs.append(level + ':' + message)
        
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
CLUSTER_REQUESTS_URL='api/v1/clusters/{0}/requests'
CLUSTER_REQUEST_URL='api/v1/clusters/{0}/requests/{1}'
CLUSTER_SERVICES_URL="api/v1/clusters/{}/services"
CLUSTER_SERVICE_URL="api/v1/clusters/{}/services/{}"
CLUSTER_COMPONENTS_URL="api/v1/clusters/{}/services/{}/components"


# JSON Keywords
CLUSTERS = 'Clusters'
ITEMS = 'items'
CLUSTER_NAME="cluster_name"
REQUESTS="Requests"
ID="id"
REQUEST_STATUS="request_status"
ITEMS="items"
SERVICE_INFO = "ServiceInfo"
SERVICE_NAME="service_name"
SERVICE_COMPONENT_INFO="ServiceComponentInfo"
COMPONENT_NAME="component_name"


class AmbariServiceApi:
    
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
        debug(url + " -> {}".format(resp.status_code)) 
        if resp.status_code == 200:
            result = resp.json()
            return result
        else:
            error("Invalid returned http code '{0}' when calling GET on '{1}'. Check ambari-server.log on the ambari node".format(resp.status_code, url))

    
    def getSafe(self, path):
        url = self.endpoint + "/" + path
        resp = requests.get(url, auth = self.auth, verify=self.verify, headers={"X-Requested-By":"ANSIBLE"})
        debug(url + " -> {}".format(resp.status_code))
        return resp.status_code, resp.text
         

    def put(self, path, body):
        url = self.endpoint + "/" + path
        #misc.ppprint(body)
        resp = requests.put(url, auth = self.auth, data=json.dumps(body), verify=self.verify, headers={"X-Requested-By":"ANSIBLE"})
        # Following does not works. Explanation may be using json=body set Content-Type in the header to application/json.
        #resp = requests.put(url, auth = self.auth, json=body, verify=self.verify, headers={"X-Requested-By":"ANSIBLE"})
        debug(url + " -> ".format(resp.status_code))
        #print(resp.text) 
        #result = resp.json()
        if resp.status_code >= 200 and resp.status_code <= 299:
            return resp.status_code, resp.text
        else:
            error("Invalid returned http code '{0}' when calling PUT on '{1}'. Check ambari-server.log on the ambari node".format(resp.status_code, url))
            

    def post(self, path, body):
        url = self.endpoint + "/" + path
        resp = requests.post(url, auth = self.auth, data=json.dumps(body), verify=self.verify, headers={"X-Requested-By":"ANSIBLE"})
        debug(url + " -> {}".format(resp.status_code)) 
        #result = resp.json()
        #print(resp.text) 
        if resp.status_code >= 200 and resp.status_code <= 299:
            return resp.status_code, resp.text
        else:
            error("Invalid returned http code '{0}' when calling POST on '{1}'. Check ambari-server.log on the ambari node".format(resp.status_code, url))

 
    def listServices(self):
        result = self.get(CLUSTER_SERVICES_URL.format(self.cluster))
        services = {}
        for srv in result[ITEMS]:
            name = srv[SERVICE_INFO][SERVICE_NAME]
            services[name] = { "components": self.listServiceComponents(name) }
        return services

    def listServiceComponents(self, service):
        result = self.get(CLUSTER_COMPONENTS_URL.format(self.cluster, service))
        components = []
        for comp in result[ITEMS]:
            components.append(comp[SERVICE_COMPONENT_INFO][COMPONENT_NAME])
        return components

    def waitTaskCompleted(self, code, responseText, wait):
        if code == 200:
            return False, None
        elif code == 201:
            return False, None
        elif code == 202:
            task = json.loads(responseText)[REQUESTS][ID]
            if wait: 
                status = "??"
                while status != "COMPLETED":
                    time.sleep(2)
                    result = self.get(CLUSTER_REQUEST_URL.format(self.cluster, task))
                    status = result[REQUESTS][REQUEST_STATUS]
                    debug(status)
            return True, task
        else:
            error("Invalid return code {} ({}) on service operation. Check ambari-server.log on the ambari node".format(code, responseText))

    def restartService(self, service, components=None, wait=True):
        if components == None:
            components = self.listServiceComponents(service)
        tasks = MySet()
        for component in components:
            debug("Restarting component {}".format(component))
            body = {
                "RequestInfo": {
                    "command":"RESTART",
                    "context":"Ansible: Restart {}/{}".format(service, component),
                    "operation_level":"host_component"
                },
                "Requests/resource_filters":[
                    {"service_name": service, "component_name": component, "hosts_predicate": "HostRoles/component_name=" + component}
                ]
            }
            code, responseText = self.post(CLUSTER_REQUESTS_URL.format(self.cluster), body)
            if code == 202:
                tasks.add(json.loads(responseText)[REQUESTS][ID])
            else:
                error("Invalid return code {} ({}) on service operation. Check ambari-server.log on the ambari node".format(code, responseText))
        allTasks = list(tasks) # Preserve for return
        if wait:
            while len(tasks) > 0:
                time.sleep(2)
                debug("Task to wait: {}".format(tasks))
                tasks2 = list(tasks) # Better to copy, as we may remove items.
                for task in tasks2:
                    result = self.get(CLUSTER_REQUEST_URL.format(self.cluster, task))
                    status = result[REQUESTS][REQUEST_STATUS]
                    debug("task:{}  status:{}".format(task, status))
                    if status == "COMPLETED":
                        tasks.remove(task)
        return allTasks
        
    def restartStaleServices(self, wait=True):
        body = {
            "RequestInfo": {
                "command":"RESTART",
                "context":"Ansible: Restart all stale services",
                "operation_level":"host_component"
            },
            "Requests/resource_filters":[
                {"hosts_predicate":"HostRoles/stale_configs=true"}
            ]
        }
        code, responseText = self.post(CLUSTER_REQUESTS_URL.format(self.cluster), body)
        return self.waitTaskCompleted(code, responseText, wait)
        
    def startService(self, service, wait=True):
        body = {
            "RequestInfo": {
                "context" :"Ansible: Start {}".format(service)
            }, 
            "Body": {
                "ServiceInfo": {"state": "STARTED"}
            }
        }
        code, responseText = self.put(CLUSTER_SERVICE_URL.format(self.cluster, service), body)
        return self.waitTaskCompleted(code, responseText, wait)

    def stopService(self, service, wait=True):
        body = {
            "RequestInfo": {
                "context" :"Ansible: Stop {}".format(service)
            }, 
            "Body": {
                "ServiceInfo": {"state": "INSTALLED"}
            }
        }
        code, responseText = self.put(CLUSTER_SERVICE_URL.format(self.cluster, service), body)
        return self.waitTaskCompleted(code, responseText, wait)
        
STARTED="started"
STOPPED="stopped"
RESTARTED="restarted"
LIST="list"
STALE="_stale_"

def main():
    global module
    module = AnsibleModule(
        argument_spec = dict(
            ambari_url = dict(type='str', required=True),
            username = dict(required=True, type='str'),
            password = dict(required=True, type='str'),
            validate_certs = dict(required=False, type='bool', default=True),
            ca_bundle_file = dict(required=False, type='str'),
            service = dict(required=False, type='str'),
            components = dict(required=False, type='raw'),
            state = dict(type='str', required=True, choices=[STARTED, STOPPED, RESTARTED, LIST]),
            wait = dict(required=False, type='bool', default=True),
            log_level = dict(type='str', required=False, default="None")
            
        ),
        supports_check_mode=False
    )
    if not HAS_REQUESTS:
        error(msg="python-requests package is not installed")    
    p = Parameters()
    p.ambariUrl = module.params['ambari_url']
    p.username = module.params['username']
    p.password = module.params['password']
    p.validateCerts = module.params['validate_certs']
    p.ca_bundleFile = module.params['ca_bundle_file']
    p.service = module.params['service']
    p.components = module.params['components']
    p.state = module.params['state']
    p.wait = module.params['wait']
    p.logLevel = module.params['log_level']
    p.checkMode = module.check_mode
    
    global  logLevel
    logLevel = p.logLevel
    
    if p.ca_bundleFile != None:
        verify = p.ca_bundleFile
    else:
        verify = p.validateCerts

    if p.state != LIST and p.service == None:
        error("Missing 'service' attribute as state != 'list'")
    
    if p.components != None and (p.state != RESTARTED or p.service == STALE):
        error("components attribute must be provided only when state == 'restarted' and service != '_stale_'")

    api = AmbariServiceApi(p.ambariUrl, p.username, p.password, verify)
    if p.state == LIST:
        services = api.listServices()
        module.exit_json(changed=False, services=services, logs=logs)
    elif p.state == STARTED:
        changed, task = api.startService(p.service, p.wait)
        module.exit_json(changed=changed, task=task, logs=logs)
    elif p.state == STOPPED:
        changed, task = api.stopService(p.service, p.wait)
        module.exit_json(changed=changed, task=task, logs=logs)
    elif p.state == RESTARTED:
        if p.service == STALE:
            changed, task = api.restartStaleServices(p.wait)
            module.exit_json(changed=changed, task=task, logs=logs)
        else:
            if p.components != None:
                if not isinstance(p.components, six.string_types):
                    p.components = json.dumps(p.components)
                components = json.loads(p.components)
            else:
                components = None
            tasks = api.restartService(p.service, components, p.wait)
            module.exit_json(changed=True, tasks=tasks, logs=logs)
    else:
        error("Unmanaged state '{}'".format(p.state))
    
    

from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()

