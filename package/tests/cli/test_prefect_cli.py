from package.cli.prefect_cli import _build_deployment_actions
from package.constants import ACTION_CREATE, ACTION_DELETE, ACTION_PAUSE, ACTION_RESUME

import unittest


class TestCreateDeployment:
    def test_create_none_and_resume_none(self):
        selected_names = []
        existing_names = []

        actual = _build_deployment_actions(ACTION_CREATE, selected_names, existing_names)
        expected = []

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_create_all(self):
        selected_names = ["dep1", "dep2"]
        existing_names = []

        actual = _build_deployment_actions(ACTION_CREATE, selected_names, existing_names)
        expected = [
            {"action": ACTION_CREATE, "name": "dep1"},
            {"action": ACTION_CREATE, "name": "dep2"},
        ]

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_resume_all(self):
        selected_names = ["dep1", "dep2"]
        existing_names = ["dep1", "dep2"]

        actual = _build_deployment_actions(ACTION_CREATE, selected_names, existing_names)
        expected = [
            {"action": ACTION_RESUME, "name": "dep1"},
            {"action": ACTION_RESUME, "name": "dep2"},
        ]

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_create_some_and_resume_some(self):
        selected_names = ["dep1", "dep2"]
        existing_names = ["dep1"]

        actual = _build_deployment_actions(ACTION_CREATE, selected_names, existing_names)
        expected = [
            {"action": ACTION_RESUME, "name": "dep1"},
            {"action": ACTION_CREATE, "name": "dep2"},
        ]

        unittest.TestCase().assertCountEqual(actual, expected)


class TestDeleteDeployment:
    def test_delete_none(self):
        selected_names = []
        existing_names = []

        actual = _build_deployment_actions(ACTION_DELETE, selected_names, existing_names)
        expected = []

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_delete_all(self):
        selected_names = ["dep1", "dep2"]
        existing_names = ["dep1", "dep2"]

        actual = _build_deployment_actions(ACTION_DELETE, selected_names, existing_names)
        expected = [
            {"action": ACTION_DELETE, "name": "dep1"},
            {"action": ACTION_DELETE, "name": "dep2"},
        ]

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_delete_some(self):
        selected_names = ["dep1"]
        existing_names = ["dep1", "dep2"]

        actual = _build_deployment_actions(ACTION_DELETE, selected_names, existing_names)
        expected = [{"action": ACTION_DELETE, "name": "dep1"}]

        unittest.TestCase().assertCountEqual(actual, expected)


class TestPauseDeployment:
    def test_pause_none(self):
        selected_names = []
        existing_names = []

        actual = _build_deployment_actions(ACTION_PAUSE, selected_names, existing_names)
        expected = []

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_pause_all(self):
        selected_names = ["dep1", "dep2"]
        existing_names = ["dep1", "dep2"]

        actual = _build_deployment_actions(ACTION_PAUSE, selected_names, existing_names)
        expected = [
            {"action": ACTION_PAUSE, "name": "dep1"},
            {"action": ACTION_PAUSE, "name": "dep2"},
        ]

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_pause_some(self):
        selected_names = ["dep1"]
        existing_names = ["dep1", "dep2"]

        actual = _build_deployment_actions(ACTION_PAUSE, selected_names, existing_names)
        expected = [{"action": ACTION_PAUSE, "name": "dep1"}]

        unittest.TestCase().assertCountEqual(actual, expected)


class TestResumeDeployment:
    def test_resume_none(self):
        selected_names = []
        existing_names = []

        actual = _build_deployment_actions(ACTION_RESUME, selected_names, existing_names)
        expected = []

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_resume_all(self):
        selected_names = ["dep1", "dep2"]
        existing_names = ["dep1", "dep2"]

        actual = _build_deployment_actions(ACTION_RESUME, selected_names, existing_names)
        expected = [
            {"action": ACTION_RESUME, "name": "dep1"},
            {"action": ACTION_RESUME, "name": "dep2"},
        ]

        unittest.TestCase().assertCountEqual(actual, expected)

    def test_resume_some(self):
        selected_names = ["dep1"]
        existing_names = ["dep1", "dep2"]

        actual = _build_deployment_actions(ACTION_RESUME, selected_names, existing_names)
        expected = [{"action": ACTION_RESUME, "name": "dep1"}]

        unittest.TestCase().assertCountEqual(actual, expected)
