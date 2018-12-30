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
module: ambari_uri
short_description: Ease management of Ambari REST API
description:
  - This module will usefully replace Ansible uri module for most if not all of Ambari API call. 
  - It will handle authentication, automatic cluster id setting and is able to wait completion for a deferred task.
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
  path:
    description:
    - The path of the command to issue. Typically starts with '/api/v1/.....
    required: true
    default: None
    aliases: []
  method:
    description:
    - HTTP Method
    choices:
    - GET
    - PUT
    - POST
    default: None
    aliases: []
  payload:
    description:
    - The request payload, as a JSON string or as a YAML definition
    default: None
    required: no
    alias: []    
  wait:
    description: Should the module wait for the operation to be completed in case of asynchronous requests 
    type: bool
    required: false
    default: True
'''

RETURN = '''
content:
  description: 
  returned: When a content is returned
  type: object
  sample: {
        "changed": false,
        "content": {
            "href": "http://sr1.hdp16:8080/api/v1/clusters/hdp16/services",
            "items": [
                {
                    "ServiceInfo": {
                        "cluster_name": "hdp16",
                        "service_name": "AMBARI_INFRA"
                    },
                    "href": "http://sr1.hdp16:8080/api/v1/clusters/hdp16/services/AMBARI_INFRA"
                },
                ...
                ...
                {
                    "ServiceInfo": {
                        "cluster_name": "hdp16",
                        "service_name": "ZOOKEEPER"
                    },
                    "href": "http://sr1.hdp16:8080/api/v1/clusters/hdp16/services/ZOOKEEPER"
                }
            ]
        },
        "failed": false,
        "http_status": 200,
        "logs": [],
    }

http_status:
  description: HTTP status code 
  returned: Always
  type: integer
  sample: 200

content2:
  description: 
  returned: Content of the last status request in case of defered command.
  type: object
  sample: {}

'''

EXAMPLES = '''

# NB: These example are for illustration. We better directly use ambari_service module to perform such task.

- hosts: sr1
  vars:
    ambari_server:
      ambari_url: "http://sr1.hdp16:8080"
      username: admin
      password: admin
  roles:
  - ambari_modules
  tasks:
  - name: Stop KAFKA
    ambari_uri:
      ambari_server: "{{ambari_server}}"
      method: "PUT"
      path: api/v1/clusters/$CLUSTER$/services/KAFKA
      payload: | 
        {
            "RequestInfo": {
                "context" :"ambari_uri: Stop KAFKA"
            }, 
            "Body": {
                "ServiceInfo": {"state": "INSTALLED"}
            }
        }
    register: ret
    changed_when: ret.http_status is defined and ret.http_status != 200
    no_log: true

  # The payload can also be expressed as a YAML block:
  - name: Start KAFKA
    ambari_uri:
      ambari_server: "{{ambari_server}}"
      method: "PUT"
      path: api/v1/clusters/$CLUSTER$/services/KAFKA
      payload:
        RequestInfo: 
          context: "ambari_uri: Start KAFKA"
        Body:
          ServiceInfo:
            state: STARTED
    register: ret
    changed_when: ret.http_status is defined and ret.http_status != 200
    no_log: true

'''

import json 
import pprint
import ansible.module_utils.six as six
import warnings
import time

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
CLUSTER_REQUEST_URL='api/v1/clusters/$CLUSTER$/requests/{}'


# JSON Keywords
CLUSTERS = 'Clusters'
ITEMS = 'items'
CLUSTER_NAME="cluster_name"
REQUESTS="Requests"
ID="id"
REQUEST_STATUS="request_status"


CLUSTER_TOKEN="$CLUSTER$"

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
        st, result = self.request("GET", CLUSTER_URL)
        if len(result[ITEMS]) != 1:
            error("Invalid response on '{}': {}".format(CLUSTER_URL, result))
        self.cluster = result[ITEMS][0][CLUSTERS][CLUSTER_NAME]
        debug("Cluster name:" + self.cluster)
    
    
    def request(self, method, path, payload=None):
        if path.startswith("/"):
            path = path[1:]
        url = self.endpoint + "/" + path
        if url.find(CLUSTER_TOKEN) != -1:
            url = url.replace(CLUSTER_TOKEN, self.cluster)
        resp = requests.request(
            method=method,
            url=url,
            json = payload,
            auth = self.auth, 
            verify=self.verify, 
            headers={"X-Requested-By":"ANSIBLE"}
        )
        debug(url + " -> {}".format(resp.status_code)) 
        if resp.status_code < 200 or resp.status_code > 299:
            error("Invalid returned http code '{0}' when calling GET on '{1}'. Check ambari-server.log on the ambari node".format(resp.status_code, url))
        else:
            if resp.status_code == 204:
                return resp.status_code, None
            else:
                try:
                    return resp.status_code, resp.json()
                except:
                    return resp.status_code, None
 
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
            path = dict(required=True, type="str"),
            method = dict(required=True, type="str", choices=["GET", "PUT", "POST"]),
            payload = dict(required=False, type="raw"),
            wait = dict(required=False, type='bool', default=True),
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
    p.ambariUrl = param2('ambari_url', module.params, ambariServer, None, False)
    p.username = param2('username', module.params, ambariServer, None, True)
    p.password = param2('password', module.params, ambariServer, None, True)
    p.validateCerts = param2('validate_certs', module.params, ambariServer, True, True)
    p.ca_bundleFile = param2('ca_bundle_file', module.params, ambariServer, None, False)
    p.path = module.params["path"]
    p.method = module.params["method"]
    p.payload = module.params["payload"]
    p.wait = module.params['wait']
    p.logLevel = module.params['log_level']
    p.checkMode = module.check_mode
    
    global  logLevel
    logLevel = p.logLevel
    
    if p.ca_bundleFile != None:
        verify = p.ca_bundleFile
    else:
        verify = p.validateCerts
    if p.payload != None:
        if not isinstance(p.payload, six.string_types):
            p.payload = json.dumps(p.payload)
    
    #debug("payload:{}".format(p.payload))
    api = AmbariServiceApi(p.ambariUrl, p.username, p.password, verify)
    
    code, content = api.request(p.method, p.path, p.payload)
    if code == 202 and p.wait:
        task = content[REQUESTS][ID]
        status = "??"
        while status != "COMPLETED":
            time.sleep(2)
            _, content2 = api.request("GET", CLUSTER_REQUEST_URL.format(task), None)
            status = content2[REQUESTS][REQUEST_STATUS]
            debug(status)
        module.exit_json(content=content, http_status=code, changed=(p.method!="GET"), content2=content2, logs=logs)
    else:
        module.exit_json(content=content, http_status=code, changed=(p.method!="GET"), logs=logs)
    
    
    

from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()

