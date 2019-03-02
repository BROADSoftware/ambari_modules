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
module: ambari_host_service
short_description: Manage stop/start of services on a given host
description:
  - This module will allow you to stop/start all services on a specific host. Typical use case is host rolling restart
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
  host:
    description:
    - The host we start/stop service on. Must be as expressed in Ambari 
    required: true
    default: None
    aliases: []    
  state:
    description:
    - Target state for all services on this host
    choices:
    - stopped
    - started
    required: true
    default: None
    aliases: []
  


'''

RETURN = '''
stopped:
  description: A list of services which was stopped.
  type: list of string
  sample:
    "stopped": [
            "DATANODE",
            "HBASE_REGIONSERVER",
            "KAFKA_BROKER",
            "METRICS_MONITOR",
            "NODEMANAGER",
            "SUPERVISOR"
        ]
          
started:
  description: A list of services which was started.
  type: list of string
  sample:
    "started": [
            "DATANODE",
            "HBASE_REGIONSERVER",
            "KAFKA_BROKER",
            "METRICS_MONITOR",
            "NODEMANAGER",
            "SUPERVISOR"
        ]

'''

EXAMPLES = '''

# Stop all services on host dn1
- hosts: sr1
  vars:
    ambariServer:
      ambari_url: "http://sr1.my.cluster.com:8080"
      username: admin
      password: admin
  roles:
  - ambari_modules
  tasks:
  - name: "Stop services on dn1"
    ambari_host_services:
      ambari_server: "{{ambariServer}}"
      host: "dn1.my.cluster.com" 
      state: "stopped"



# Start all services on host dn1.
# Note we setup a retry mechanism, as this task will fail if the host is temporary unsynchronized with Ambari (Heartbeat lost).  
# This will typically occurs after a host reboot.
- hosts: sr1
  vars:
    ambariServer:
      ambari_url: "http://sr1.my.cluster.com:8080"
      username: admin
      password: admin
  roles:
  - ambari_modules
  tasks:
  - name: "Start services on dn1"
    ambari_host_services:
      ambari_server: "{{ambariServer}}"
      host: "dn1.my.cluster.com" 
      state: "started"
    register: startService
    retries: 30
    delay: 10
    until: not startService.failed
    
'''

import pprint
import ansible.module_utils.six as six
import warnings
import time
import json

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
CLUSTER_REQUESTS_URL='api/v1/clusters/{0}/requests'
CLUSTER_REQUEST_URL='api/v1/clusters/{0}/requests/{1}'
LIST_HOST_COMPONENTS_URL = 'api/v1/clusters/{0}/hosts/{1}/host_components'
HOST_COMPONENTS_URL = 'api/v1/clusters/{0}/hosts/{1}/host_components/{2}'
GET_HOST_COMPONENTS_INFO_URL = 'api/v1/clusters/{0}/hosts/{1}/host_components/{2}?fields={3}'


# JSON Keywords
CLUSTERS = 'Clusters'
ITEMS = 'items'
CLUSTER_NAME="cluster_name"
REQUESTS="Requests"
ID="id"
REQUEST_STATUS="request_status"
ITEMS="items"


class AmbariServiceApi:
    
    def __init__(self, endpoint, username, password, verify, host):
        if endpoint.endswith("/"):
            endpoint = endpoint[:-1]
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.verify = verify
        self.host = host
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
            error("Invalid returned http code '{0}' when calling GET on '{1}'. Check ambari-server.log on the ambari node".format(resp.status_code, url))

    
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
            error("Invalid returned http code '{0}' when calling PUT on '{1}'. Check ambari-server.log on the ambari node".format(resp.status_code, url))
            
    def listComponents(self):
        r = self.get(LIST_HOST_COMPONENTS_URL.format(self.cluster, self.host))
        components = []
        for item in r["items"]:
            if "HostRoles" in item:
                if "component_name" in item["HostRoles"]:
                    #debug("component:{}".format(item["HostRoles"]["component_name"]))
                    components.append(item["HostRoles"]["component_name"])
        return components            

    def getComponentInfo(self, component, fields):
        r = self.get(GET_HOST_COMPONENTS_INFO_URL.format(self.cluster, self.host, component, fields))
        return r

    def getComponentStateInfo(self, component):
        x = self.getComponentInfo(component, "HostRoles/desired_state,HostRoles/state,HostRoles/component_name,HostRoles/desired_admin_state")
        r = {}
        r["component_name"] = x["HostRoles"]["component_name"]
        r["desired_state"] = x["HostRoles"]["desired_state"]
        r["state"] = x["HostRoles"]["state"]
        if "desired_admin_state" in x["HostRoles"]:
            r["desired_admin_state"] = x["HostRoles"]["desired_admin_state"]
        return r
            
    def stopHostComponent(self, component):
        return self.put(HOST_COMPONENTS_URL.format(self.cluster, self.host, component), { "HostRoles": {"state": "INSTALLED"}, "RequestInfo": { "context" :"Ansible: Stop {}".format(component) } } )

    def startHostComponent(self, component):
        return self.put(HOST_COMPONENTS_URL.format(self.cluster, self.host, component), { "HostRoles": {"state": "STARTED"}, "RequestInfo": { "context" :"Ansible: Start {}".format(component) } } )
        
    def waitTaskCompletion(self, taskIds):
        statusList = []
        for taskId in taskIds:
            status = "IN_PROGRESS"
            while status == "IN_PROGRESS":
                time.sleep(1)
                result = self.get(CLUSTER_REQUEST_URL.format(self.cluster, taskId))
                status = result[REQUESTS][REQUEST_STATUS]
                #print("id:{}  status:{}".format(taskId, status))
            statusList.append(status)
        return statusList 
                
def startComponents(api):    
    started = []
    components = api.listComponents()
    toWait = []
    for component in components:
        state = api.getComponentStateInfo(component)
        if state["state"] == "STARTED" or state["state"] == "STARTING" or state["state"] == "DISABLED":
            pass
        elif state["state"] == "INSTALLED":
            code, body = api.startHostComponent(component)
            debug("Start {} -> {}  ({})".format(component, code, body))
            if code == 202:
                started.append(component)       # Only defered task are effective start. Others are client components
                toWait.append(json.loads(body)["Requests"]["id"])
        else:
            error("Component {} in invalid state: {}".format(component, state["state"]))
    debug("taskToWait: {}".format(str(toWait)))
    taskResults = api.waitTaskCompletion(toWait)
    debug("taskResults: {}".format(str(taskResults)))
    for taskResult  in taskResults:
        if taskResult != "COMPLETED":
            error("At least one task has not completed (result:{})".format(taskResult))
    return started

def stopComponents(api):
    stopped = []
    components = api.listComponents()
    toWait = []
    for component in components:
        state = api.getComponentStateInfo(component)
        if state["state"] == "INSTALLED" or state["state"] == "STOPPING" or state["state"] == "DISABLED":
            pass
        elif state["state"] == "STARTED":
            stopped.append(component)
            code, body = api.stopHostComponent(component)
            debug("Stop {} -> {}  ({})".format(component, code, body))
            if code == 202:
                toWait.append(json.loads(body)["Requests"]["id"])
        else:
            error("Component {} in invalid state: {}".format(component, state["state"]))
    debug("taskToWait: {}".format(str(toWait)))
    taskResults = api.waitTaskCompletion(toWait)
    debug("taskResults: {}".format(str(taskResults)))
    for taskResult  in taskResults:
        if taskResult != "COMPLETED":
            error("At least one task has not completed (result:{})".format(taskResult))
    return stopped

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
            host = dict(required=True, type='str'),
            state = dict(type='str', required=True, choices=["started", "stopped"]),
            log_level = dict(type='str', required=False, default="None")
        ),
        supports_check_mode=False
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
    p.host = module.params['host']
    p.state = module.params['state']
    p.logLevel = module.params['log_level']
    p.checkMode = module.check_mode
    
    global  logLevel
    logLevel = p.logLevel
    
    if p.ca_bundleFile != None:
        verify = p.ca_bundleFile
    else:
        verify = p.validateCerts

    api = AmbariServiceApi(p.ambariUrl, p.username, p.password, verify, p.host)
    
    if p.state == "started":
        started = startComponents(api)
        module.exit_json(changed=(len(started) > 0), started=started, logs=logs)
    elif p.state == "stopped":
        stopped = stopComponents(api)
        module.exit_json(changed=(len(stopped) > 0), stopped=stopped, logs=logs)
    else:
        error("Invalid 'state' parameter value: {}".format(p.state))

from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()

