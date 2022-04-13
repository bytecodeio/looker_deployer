# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from looker_deployer.commands import deploy_code
import requests

CONFIG_NO_EXCLUDE = {
    "instances": [
        {
            "name": "foo",
            "endpoint": "bar",
            "spoke_project": "baz"
        }
    ],
    "hub_project": "bosh"
}

CONFIG_WITH_EXCLUDE = {
    "instances": [
        {
            "name": "foo",
            "endpoint": "bar",
            "spoke_project": "baz"
        },
        {
            "name": "taco",
            "endpoint": "burrito",
            "spoke_project": "bananna"
        }
    ],
    "hub_project": "bosh",
    "hub_deploy_exclude": ["taco"]
}

GOOD_RESPONSE = requests.Response()
GOOD_RESPONSE.status_code = 200
BAD_RESPONSE = requests.Response()
BAD_RESPONSE.status_code = 500
RESP_JSON = {
    "operations": [
        {"results": ["success"]}
    ]
}


def test_parse_hub_endpoints():

    endpoints = deploy_code.parse_hub_endpoints(CONFIG_NO_EXCLUDE)
    assert endpoints == ["bar"]


def test_parse_hub_endpoints_with_exclude():
    endpoints = deploy_code.parse_hub_endpoints(CONFIG_WITH_EXCLUDE)
    assert endpoints == ["bar"]


def test_parse_spoke_config():
    spoke_config = deploy_code.parse_spoke_config("foo", CONFIG_NO_EXCLUDE)
    assert spoke_config == {"name": "foo", "endpoint": "bar", "spoke_project": "baz"}


def test_parse_spoke_config_no_val():
    with pytest.raises(IndexError):
        deploy_code.parse_spoke_config("flurb", CONFIG_NO_EXCLUDE)


def test_parse_hub_excludes():
    test_config = CONFIG_NO_EXCLUDE
    deploy_code.parse_hub_excludes(test_config, ["taco"])
    assert test_config["hub_deploy_exclude"] == ["taco"]


def test_parse_hub_excludes_with_exclude():
    test_config = CONFIG_WITH_EXCLUDE
    deploy_code.parse_hub_excludes(test_config, ["foo"])
    assert test_config["hub_deploy_exclude"] == ["taco", "foo"]


def test_deploy_code(mocker):
    mocker.patch("requests.get")
    mocker.patch("requests.Response.json")
    requests.get.return_value = GOOD_RESPONSE
    requests.Response.json.return_value = RESP_JSON
    deploy_code.deploy_code("foo", "bar", "bash")
    requests.get.assert_called_with("bar/webhooks/projects/foo/deploy", headers="bash")


def test_deploy_code_assertion_error(mocker):
    mocker.patch("requests.get")
    requests.get.return_value = BAD_RESPONSE
    with pytest.raises(AssertionError):
        deploy_code.deploy_code("foo", "bar", "baz")
