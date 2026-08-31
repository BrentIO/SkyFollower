"""
Tests for aws-setup/main.py that don't require live AWS. A fake
CloudFormation client stands in for boto3, so the change-set flow,
mode dispatch, and the stdout(=KEY=value)/stderr(=everything else)
separation can all be exercised offline.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys

import pytest
from botocore.exceptions import ClientError, WaiterError

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.abspath(os.path.join(_TOOL_DIR, ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "aws_setup_main", os.path.join(_TOOL_DIR, "main.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["aws_setup_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


def _client_error(code: str, message: str, op: str = "DescribeStacks") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, op)


class _Waiter:
    def __init__(self, fail: bool = False) -> None:
        self._fail = fail

    def wait(self, **_kwargs) -> None:
        if self._fail:
            raise WaiterError("stack", "reason", {})


class FakeCloudFormation:
    """Minimal stand-in for boto3's cloudformation client."""

    def __init__(
        self,
        *,
        exists: bool = False,
        stack_status: str = "CREATE_COMPLETE",
        change_set_status: str = "CREATE_COMPLETE",
        change_set_reason: str = "",
        changes: list | None = None,
        outputs: dict | None = None,
        waiter_fails: bool = False,
        failure_reason: str = "",
    ) -> None:
        self._exists = exists
        self._stack_status = stack_status
        self._change_set_status = change_set_status
        self._change_set_reason = change_set_reason
        self._changes = changes if changes is not None else [
            {"ResourceChange": {"Action": "Add", "LogicalResourceId": "GlueDatabase",
                                "ResourceType": "AWS::Glue::Database", "Replacement": "False"}}
        ]
        self._outputs = outputs or {"ArchiveBucketName": "b", "AwsRegion": "us-east-1"}
        self._waiter_fails = waiter_fails
        self._failure_reason = failure_reason
        self.calls: list[str] = []

    def describe_stacks(self, StackName):
        self.calls.append("describe_stacks")
        if not self._exists:
            raise _client_error("ValidationError", f"Stack with id {StackName} does not exist")
        return {"Stacks": [{
            "StackStatus": self._stack_status,
            "Outputs": [{"OutputKey": k, "OutputValue": v} for k, v in self._outputs.items()],
        }]}

    def create_change_set(self, **kwargs):
        self.calls.append("create_change_set")
        assert kwargs["Capabilities"] == ["CAPABILITY_NAMED_IAM"]
        self.create_change_set_role_arn = kwargs.get("RoleARN")
        return {"Id": "cs-1"}

    def describe_change_set(self, ChangeSetName, StackName):
        self.calls.append("describe_change_set")
        return {
            "Status": self._change_set_status,
            "StatusReason": self._change_set_reason,
            "Changes": self._changes,
        }

    def delete_change_set(self, **kwargs):
        self.calls.append("delete_change_set")

    def execute_change_set(self, **kwargs):
        self.calls.append("execute_change_set")
        self.execute_change_set_role_arn = kwargs.get("RoleARN")
        self._exists = True

    def delete_stack(self, **kwargs):
        self.calls.append("delete_stack")

    def describe_stack_events(self, StackName):
        self.calls.append("describe_stack_events")
        if not self._failure_reason:
            return {"StackEvents": []}
        return {"StackEvents": [{
            "ResourceStatus": "CREATE_FAILED",
            "LogicalResourceId": "ArchiveBucket",
            "ResourceStatusReason": self._failure_reason,
        }]}

    def get_waiter(self, name):
        self.calls.append(f"get_waiter:{name}")
        return _Waiter(fail=self._waiter_fails)


_ROLE_ARN = "arn:aws:iam::123456789012:role/skyfollower-cloudformation-execution"


class FakeIamRole:
    """Stand-in for the IAM client's role calls used by ensure_execution_role
    / delete_execution_role."""

    def __init__(self, *, exists=False, existing_policy=None,
                 create_fails_exists=None) -> None:
        # create_fails_exists defaults to `exists` -- a role that already
        # exists makes create_role raise EntityAlreadyExists.
        self._exists = exists
        self._create_fails_exists = exists if create_fails_exists is None else create_fails_exists
        self._policy = existing_policy
        self.calls: list[str] = []
        self.put_policy_document = None

    def create_role(self, **kwargs):
        self.calls.append("create_role")
        if self._create_fails_exists:
            raise _client_error("EntityAlreadyExists", "exists", "CreateRole")
        self._exists = True
        return {"Role": {"Arn": _ROLE_ARN}}

    def get_role(self, RoleName):
        self.calls.append("get_role")
        if not self._exists:
            raise _client_error("NoSuchEntity", "missing", "GetRole")
        return {"Role": {"Arn": _ROLE_ARN}}

    def update_assume_role_policy(self, **kwargs):
        self.calls.append("update_assume_role_policy")

    def get_role_policy(self, RoleName, PolicyName):
        self.calls.append("get_role_policy")
        if self._policy is None:
            raise _client_error("NoSuchEntity", "missing", "GetRolePolicy")
        return {"PolicyDocument": self._policy}

    def put_role_policy(self, RoleName, PolicyName, PolicyDocument):
        self.calls.append("put_role_policy")
        self.put_policy_document = json.loads(PolicyDocument)
        self._policy = json.loads(PolicyDocument)

    def delete_role_policy(self, RoleName, PolicyName):
        self.calls.append("delete_role_policy")
        if self._policy is None:
            raise _client_error("NoSuchEntity", "missing", "DeleteRolePolicy")
        self._policy = None

    def delete_role(self, RoleName):
        self.calls.append("delete_role")
        if not self._exists:
            raise _client_error("NoSuchEntity", "missing", "DeleteRole")
        self._exists = False


@pytest.fixture(autouse=True)
def _no_role_propagation_sleep(monkeypatch):
    monkeypatch.setattr(_mod.time, "sleep", lambda *_a, **_k: None)


# ---------------------------------------------------------------------------
# build_parameters
# ---------------------------------------------------------------------------

class TestBuildParameters:
    def test_missing_required_raises(self):
        with pytest.raises(SystemExit) as exc:
            _mod.build_parameters({})
        assert "ARCHIVE_BUCKET_NAME" in str(exc.value)

    def test_required_only(self):
        params = _mod.build_parameters({"ARCHIVE_BUCKET_NAME": "my-bucket"})
        assert params == [{"ParameterKey": "ArchiveBucketName", "ParameterValue": "my-bucket"}]

    def test_optional_passed_through_when_present(self):
        params = _mod.build_parameters({
            "ARCHIVE_BUCKET_NAME": "b",
            "CREATE_ARCHIVE_BUCKET": "No",
            "RESOURCE_NAME_PREFIX": "sf-test",
            "ACCESS_KEY_SERIAL": "3",
        })
        as_dict = {p["ParameterKey"]: p["ParameterValue"] for p in params}
        assert as_dict == {
            "ArchiveBucketName": "b",
            "CreateArchiveBucket": "No",
            "ResourceNamePrefix": "sf-test",
            "AccessKeySerial": "3",
        }

    def test_blank_optional_is_omitted(self):
        params = _mod.build_parameters({"ARCHIVE_BUCKET_NAME": "b", "GLUE_TABLE_NAME": "  "})
        assert [p["ParameterKey"] for p in params] == ["ArchiveBucketName"]


# ---------------------------------------------------------------------------
# change-set helpers
# ---------------------------------------------------------------------------

class TestChangeSetHelpers:
    def test_empty_change_set_detected(self):
        assert _mod._change_set_is_empty(
            {"Status": "FAILED", "StatusReason": "The submitted information didn't contain changes."}
        )
        assert _mod._change_set_is_empty(
            {"Status": "FAILED", "StatusReason": "No updates are to be performed."}
        )

    def test_real_failure_not_treated_as_empty(self):
        assert not _mod._change_set_is_empty(
            {"Status": "FAILED", "StatusReason": "Template format error: unresolved resource"}
        )
        assert not _mod._change_set_is_empty({"Status": "CREATE_COMPLETE", "StatusReason": ""})

    def test_has_replacement(self):
        assert _mod.has_replacement([{"ResourceChange": {"Replacement": "True"}}])
        assert _mod.has_replacement([{"ResourceChange": {"Replacement": "Conditional"}}])
        assert not _mod.has_replacement([{"ResourceChange": {"Replacement": "False"}}])
        assert not _mod.has_replacement([{"ResourceChange": {"Action": "Add"}}])

    def test_summary_marks_replacements(self):
        text = _mod.summarize_changes([
            {"ResourceChange": {"Action": "Modify", "LogicalResourceId": "ManagementUiUser",
                                "ResourceType": "AWS::IAM::User", "Replacement": "True"}},
        ])
        assert "Replacement: True" in text
        assert "ManagementUiUser" in text


# ---------------------------------------------------------------------------
# stack_exists
# ---------------------------------------------------------------------------

class TestStackExists:
    def test_missing_stack(self):
        assert _mod.stack_exists(FakeCloudFormation(exists=False), "skyfollower") is False

    def test_present_stack(self):
        assert _mod.stack_exists(FakeCloudFormation(exists=True), "skyfollower") is True

    def test_review_in_progress_is_not_exists(self):
        cf = FakeCloudFormation(exists=True, stack_status="REVIEW_IN_PROGRESS")
        assert _mod.stack_exists(cf, "skyfollower") is False

    def test_unexpected_client_error_propagates(self):
        class Boom(FakeCloudFormation):
            def describe_stacks(self, StackName):
                raise _client_error("AccessDenied", "not allowed")

        with pytest.raises(ClientError):
            _mod.stack_exists(Boom(), "skyfollower")


# ---------------------------------------------------------------------------
# emit_outputs / stdout separation
# ---------------------------------------------------------------------------

class TestEmitOutputs:
    def test_key_value_lines_sorted_on_stdout(self, capsys):
        _mod.emit_outputs({"Bravo": "2", "Alpha": "1"})
        captured = capsys.readouterr()
        assert captured.out == "Alpha=1\nBravo=2\n"
        assert captured.err == ""

    def test_log_goes_to_stderr(self, capsys):
        _mod.log("progress")
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "progress" in captured.err


# ---------------------------------------------------------------------------
# provision
# ---------------------------------------------------------------------------

class TestProvision:
    def test_create_executes_without_prompt_when_no_replacement(self, capsys):
        cf = FakeCloudFormation(exists=False, outputs={"ArchiveBucketName": "b"})
        out = _mod.provision(cf, "skyfollower", "body", [], _ROLE_ARN, assume_yes=False, interactive=False)
        assert out == {"ArchiveBucketName": "b"}
        assert "execute_change_set" in cf.calls
        assert "get_waiter:stack_create_complete" in cf.calls

    def test_role_arn_threaded_into_both_change_set_calls(self):
        cf = FakeCloudFormation(exists=False, outputs={"ArchiveBucketName": "b"})
        _mod.provision(cf, "skyfollower", "body", [], _ROLE_ARN, assume_yes=False, interactive=False)
        assert cf.create_change_set_role_arn == _ROLE_ARN
        assert cf.execute_change_set_role_arn == _ROLE_ARN

    def test_noop_update_returns_outputs(self, capsys):
        cf = FakeCloudFormation(
            exists=True, change_set_status="FAILED",
            change_set_reason="The submitted information didn't contain changes.",
            outputs={"ArchiveBucketName": "b", "AwsRegion": "eu-west-1"},
        )
        out = _mod.provision(cf, "skyfollower", "body", [], _ROLE_ARN, assume_yes=False, interactive=False)
        assert out["AwsRegion"] == "eu-west-1"
        assert "execute_change_set" not in cf.calls
        assert "delete_change_set" in cf.calls

    def test_replacement_without_yes_non_interactive_aborts(self):
        cf = FakeCloudFormation(
            exists=True,
            changes=[{"ResourceChange": {"Action": "Modify", "LogicalResourceId": "ManagementUiUser",
                                         "ResourceType": "AWS::IAM::User", "Replacement": "True"}}],
        )
        with pytest.raises(SystemExit) as exc:
            _mod.provision(cf, "skyfollower", "body", [], _ROLE_ARN, assume_yes=False, interactive=False)
        assert "--yes" in str(exc.value)
        assert "execute_change_set" not in cf.calls

    def test_replacement_with_yes_executes(self):
        cf = FakeCloudFormation(
            exists=True,
            changes=[{"ResourceChange": {"Action": "Modify", "LogicalResourceId": "ManagementUiUser",
                                         "ResourceType": "AWS::IAM::User", "Replacement": "True"}}],
        )
        _mod.provision(cf, "skyfollower", "body", [], _ROLE_ARN, assume_yes=True, interactive=False)
        assert "execute_change_set" in cf.calls

    def test_waiter_failure_prints_reason_and_exits(self, capsys):
        cf = FakeCloudFormation(exists=False, waiter_fails=True,
                                failure_reason="BucketAlreadyOwnedByYou")
        with pytest.raises(SystemExit):
            _mod.provision(cf, "skyfollower", "body", [], _ROLE_ARN, assume_yes=False, interactive=False)
        assert "BucketAlreadyOwnedByYou" in capsys.readouterr().err

    def test_empty_create_change_set_is_an_error(self):
        cf = FakeCloudFormation(
            exists=False, change_set_status="FAILED",
            change_set_reason="The submitted information didn't contain changes.",
        )
        with pytest.raises(SystemExit) as exc:
            _mod.provision(cf, "skyfollower", "body", [], _ROLE_ARN, assume_yes=False, interactive=False)
        assert "does not exist" in str(exc.value)


# ---------------------------------------------------------------------------
# main() mode dispatch
# ---------------------------------------------------------------------------

class TestMain:
    def test_outputs_only(self, capsys, monkeypatch):
        cf = FakeCloudFormation(exists=True, outputs={"AwsRegion": "us-east-2", "GlueTableName": "archive_flights"})
        monkeypatch.setattr(_mod.boto3, "client", lambda _name: cf)
        rc = _mod.main(["--outputs-only"])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out == "AwsRegion=us-east-2\nGlueTableName=archive_flights\n"
        assert "create_change_set" not in cf.calls

    def test_delete_mode(self, capsys, monkeypatch):
        cf = FakeCloudFormation(exists=True)
        iam = FakeIamRole(exists=True, existing_policy={"x": 1})
        monkeypatch.setattr(_mod.boto3, "client",
                            lambda name: iam if name == "iam" else cf)
        rc = _mod.main(["--delete"])
        assert rc == 0
        assert "delete_stack" in cf.calls
        assert "delete_role" in iam.calls
        assert capsys.readouterr().out == ""

    def test_default_mode_reads_template_and_emits_outputs(self, capsys, monkeypatch):
        cf = FakeCloudFormation(exists=False, outputs={"ArchiveBucketName": "b"})
        iam = FakeIamRole(exists=False)
        monkeypatch.setattr(_mod.boto3, "client",
                            lambda name: iam if name == "iam" else cf)
        monkeypatch.setenv("ARCHIVE_BUCKET_NAME", "b")
        rc = _mod.main([])
        assert rc == 0
        assert capsys.readouterr().out == "ArchiveBucketName=b\n"
        assert "create_role" in iam.calls
        assert cf.create_change_set_role_arn == _ROLE_ARN

    def test_stack_name_override(self, monkeypatch):
        cf = FakeCloudFormation(exists=True, outputs={"AwsRegion": "x"})
        seen = {}
        orig = cf.describe_stacks

        def spy(StackName):
            seen["name"] = StackName
            return orig(StackName)

        cf.describe_stacks = spy
        monkeypatch.setattr(_mod.boto3, "client", lambda _name: cf)
        monkeypatch.setenv("STACK_NAME", "skyfollower-test")
        _mod.main(["--outputs-only"])
        assert seen["name"] == "skyfollower-test"


# ---------------------------------------------------------------------------
# The template the container ships is the one the anti-drift test guards
# ---------------------------------------------------------------------------

def test_template_path_resolves_to_repo_spec():
    assert os.path.basename(_mod._TEMPLATE_PATH) == "cloudformation.yaml"
    assert os.path.exists(_mod._TEMPLATE_PATH)


# ---------------------------------------------------------------------------
# ensure_execution_role / delete_execution_role
# ---------------------------------------------------------------------------

class TestEnsureExecutionRole:
    def test_create_when_absent_then_attaches_policy(self, capsys):
        iam = FakeIamRole(exists=False)
        arn = _mod.ensure_execution_role(iam, {"ARCHIVE_BUCKET_NAME": "b"})
        assert arn == _ROLE_ARN
        assert "create_role" in iam.calls
        assert "put_role_policy" in iam.calls
        assert iam.put_policy_document == _mod.build_execution_role_policy(
            {"ARCHIVE_BUCKET_NAME": "b"}
        )
        assert "Attached" in capsys.readouterr().err

    def test_noop_when_role_and_policy_already_match(self, capsys):
        env = {"ARCHIVE_BUCKET_NAME": "b", "RESOURCE_NAME_PREFIX": "sf"}
        iam = FakeIamRole(exists=True,
                          existing_policy=_mod.build_execution_role_policy(env))
        arn = _mod.ensure_execution_role(iam, env)
        assert arn == _ROLE_ARN
        assert "create_role" in iam.calls  # attempted, raised EntityAlreadyExists
        assert "get_role" in iam.calls
        assert "put_role_policy" not in iam.calls
        assert "already up to date" in capsys.readouterr().err

    def test_updates_policy_in_place_when_changed(self, capsys):
        env = {"ARCHIVE_BUCKET_NAME": "b"}
        iam = FakeIamRole(exists=True, existing_policy={"Version": "2012-10-17",
                                                       "Statement": [{"Sid": "stale"}]})
        _mod.ensure_execution_role(iam, env)
        assert "put_role_policy" in iam.calls
        assert iam.put_policy_document == _mod.build_execution_role_policy(env)
        assert "Updated" in capsys.readouterr().err

    def test_no_role_propagation_sleep_when_role_already_existed(self, monkeypatch):
        slept = []
        monkeypatch.setattr(_mod.time, "sleep", lambda s: slept.append(s))
        env = {"ARCHIVE_BUCKET_NAME": "b"}
        iam = FakeIamRole(exists=True,
                          existing_policy=_mod.build_execution_role_policy(env))
        _mod.ensure_execution_role(iam, env)
        assert slept == []

    def test_role_name_follows_prefix(self):
        assert _mod.execution_role_name({}) == "skyfollower-cloudformation-execution"
        assert _mod.execution_role_name({"RESOURCE_NAME_PREFIX": "sf"}) == \
            "sf-cloudformation-execution"


class TestDeleteExecutionRole:
    def test_deletes_policy_then_role(self, capsys):
        iam = FakeIamRole(exists=True, existing_policy={"x": 1})
        _mod.delete_execution_role(iam, "skyfollower-cloudformation-execution")
        assert iam.calls == ["delete_role_policy", "delete_role"]
        assert "Deleted" in capsys.readouterr().err

    def test_missing_role_is_not_fatal(self, capsys):
        iam = FakeIamRole(exists=False, existing_policy=None)
        _mod.delete_execution_role(iam, "skyfollower-cloudformation-execution")
        assert "already gone" in capsys.readouterr().err

    def test_delete_failure_is_reported_not_raised(self, capsys):
        class Boom(FakeIamRole):
            def delete_role(self, RoleName):
                raise _client_error("DeleteConflict", "still attached", "DeleteRole")

        iam = Boom(exists=True, existing_policy={"x": 1})
        _mod.delete_execution_role(iam, "skyfollower-cloudformation-execution")
        err = capsys.readouterr().err
        assert "left in place" in err


# ---------------------------------------------------------------------------
# build_execution_role_policy
# ---------------------------------------------------------------------------

class TestExecutionRolePolicy:
    def test_no_cloudformation_or_passrole_actions(self):
        policy = _mod.build_execution_role_policy({"ARCHIVE_BUCKET_NAME": "b"})
        blob = json.dumps(policy)
        assert "cloudformation:" not in blob
        assert "iam:PassRole" not in blob
        assert "iam:CreateRole" not in blob

    def test_archive_bucket_has_no_delete_bucket(self):
        policy = _mod.build_execution_role_policy({
            "ARCHIVE_BUCKET_NAME": "sf-archive",
            "AWS_DEFAULT_REGION": "eu-west-1",
            "AWS_ACCOUNT_ID": "123456789012",
            "RESOURCE_NAME_PREFIX": "sf",
        })
        sids = {s["Sid"]: s for s in policy["Statement"]}
        assert sids["ArchiveBucketCreateAndConfigure"]["Resource"] == "arn:aws:s3:::sf-archive"
        assert "s3:DeleteBucket" not in sids["ArchiveBucketCreateAndConfigure"]["Action"]
        assert "s3:DeleteBucket" in sids["AthenaResultsBucketCreateConfigureAndDelete"]["Action"]
        assert sids["AthenaWorkGroup"]["Resource"] == \
            "arn:aws:athena:eu-west-1:123456789012:workgroup/skyfollower"
        assert sids["ProvisionedIamUsers"]["Resource"] == [
            "arn:aws:iam::123456789012:user/sf-archive-processor",
            "arn:aws:iam::123456789012:user/sf-archive-compaction",
            "arn:aws:iam::123456789012:user/sf-management-ui",
        ]

    def test_fully_substituted_no_placeholders_without_bucket(self):
        # Execution-role policy renders even with no bucket name (the doc
        # copy is generated this way).
        blob = json.dumps(_mod.build_execution_role_policy({}))
        assert "${" not in blob and "PLACEHOLDER" not in blob


# ---------------------------------------------------------------------------
# --print-bootstrap-policy / build_bootstrap_policy
# ---------------------------------------------------------------------------

class TestBootstrapPolicy:
    def test_renders_without_credentials(self):
        policy = _mod.build_bootstrap_policy({
            "AWS_DEFAULT_REGION": "eu-west-1",
            "RESOURCE_NAME_PREFIX": "sf",
        })
        blob = json.dumps(policy)
        assert "${" not in blob and "PLACEHOLDER" not in blob
        assert policy["Version"] == "2012-10-17"

    def test_control_plane_scoped_to_stack(self):
        policy = _mod.build_bootstrap_policy({
            "AWS_DEFAULT_REGION": "eu-west-1",
            "AWS_ACCOUNT_ID": "123456789012",
            "STACK_NAME": "skyfollower",
        })
        sids = {s["Sid"]: s for s in policy["Statement"]}
        cp = sids["CloudFormationControlPlane"]
        assert cp["Resource"] == [
            "arn:aws:cloudformation:eu-west-1:123456789012:stack/skyfollower/*",
            "arn:aws:cloudformation:eu-west-1:123456789012:changeSet/skyfollower-*/*",
        ]
        assert "cloudformation:ExecuteChangeSet" in cp["Action"]

    def test_passrole_scoped_to_one_execution_role(self):
        policy = _mod.build_bootstrap_policy({
            "AWS_ACCOUNT_ID": "123456789012",
            "RESOURCE_NAME_PREFIX": "sf",
        })
        stmt = next(s for s in policy["Statement"] if s["Sid"] == "ManageAndPassExecutionRole")
        assert stmt["Resource"] == \
            "arn:aws:iam::123456789012:role/sf-cloudformation-execution"
        for action in ("iam:PassRole", "iam:CreateRole", "iam:PutRolePolicy",
                       "iam:DeleteRole"):
            assert action in stmt["Action"]

    def test_self_cleanup_statement_scoped_to_one_named_user(self):
        policy = _mod.build_bootstrap_policy({
            "RESOURCE_NAME_PREFIX": "sf",
            "BOOTSTRAP_USER_NAME": "sf-bootstrap",
        })
        cleanup = next(s for s in policy["Statement"] if s["Sid"] == "BootstrapUserSelfCleanup")
        assert cleanup["Resource"] == "arn:aws:iam::*:user/sf-bootstrap"
        for action in ("iam:DeleteAccessKey", "iam:ListAccessKeys",
                       "iam:DeleteUserPolicy", "iam:DeleteUser"):
            assert action in cleanup["Action"]

    def test_main_prints_policy_json_to_stdout(self, capsys, monkeypatch):
        monkeypatch.setattr(_mod.boto3, "client", lambda _name: (_ for _ in ()).throw(
            AssertionError("no AWS client should be built for --print-bootstrap-policy")))
        rc = _mod.main(["--print-bootstrap-policy"])
        assert rc == 0
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["Statement"][0]["Sid"] == "CloudFormationControlPlane"
        assert captured.err == ""


# ---------------------------------------------------------------------------
# --delete-bootstrap-user
# ---------------------------------------------------------------------------

class FakeIam:
    def __init__(self, *, keys=None, policies=None, attached=None,
                 fail_on: set | None = None, missing: bool = False) -> None:
        self._keys = keys or []
        self._policies = policies or []
        self._attached = attached or []
        self._fail_on = fail_on or set()
        self._missing = missing
        self.calls: list[str] = []

    def _maybe_fail(self, op):
        if op in self._fail_on:
            raise _client_error("AccessDenied", f"denied: {op}", op)
        if self._missing:
            raise _client_error("NoSuchEntity", "not found", op)

    def list_access_keys(self, UserName):
        self.calls.append("list_access_keys")
        self._maybe_fail("list_access_keys")
        return {"AccessKeyMetadata": [{"AccessKeyId": k} for k in self._keys]}

    def delete_access_key(self, UserName, AccessKeyId):
        self.calls.append(f"delete_access_key:{AccessKeyId}")
        self._maybe_fail("delete_access_key")

    def list_user_policies(self, UserName):
        self.calls.append("list_user_policies")
        self._maybe_fail("list_user_policies")
        return {"PolicyNames": list(self._policies)}

    def delete_user_policy(self, UserName, PolicyName):
        self.calls.append(f"delete_user_policy:{PolicyName}")
        self._maybe_fail("delete_user_policy")

    def list_attached_user_policies(self, UserName):
        self.calls.append("list_attached_user_policies")
        return {"AttachedPolicies": [{"PolicyArn": a} for a in self._attached]}

    def detach_user_policy(self, UserName, PolicyArn):
        self.calls.append(f"detach_user_policy:{PolicyArn}")

    def delete_user(self, UserName):
        self.calls.append("delete_user")
        self._maybe_fail("delete_user")


class TestDeleteBootstrapUser:
    def test_happy_path_deletes_keys_policies_then_user(self, capsys):
        iam = FakeIam(keys=["AKIA1", "AKIA2"], policies=["sf-bootstrap-policy"])
        _mod.delete_bootstrap_user(iam, "sf-bootstrap")
        assert "delete_access_key:AKIA1" in iam.calls
        assert "delete_access_key:AKIA2" in iam.calls
        assert "delete_user_policy:sf-bootstrap-policy" in iam.calls
        assert iam.calls[-1] == "delete_user"
        assert "deleted" in capsys.readouterr().err

    def test_missing_user_is_a_clean_noop(self, capsys):
        iam = FakeIam(missing=True)
        _mod.delete_bootstrap_user(iam, "gone")
        assert "does not exist" in capsys.readouterr().err

    def test_failure_lists_what_remains_and_exits_nonzero(self, capsys):
        iam = FakeIam(keys=["AKIA1"], policies=["p"], fail_on={"delete_user"})
        with pytest.raises(SystemExit):
            _mod.delete_bootstrap_user(iam, "sf-bootstrap")
        err = capsys.readouterr().err
        assert "Still present" in err
        assert "sf-bootstrap" in err
        assert "IAM console" in err

    def test_main_delete_bootstrap_user_uses_iam_client(self, monkeypatch):
        iam = FakeIam(keys=["AKIA1"])
        monkeypatch.setattr(_mod.boto3, "client",
                            lambda name: iam if name == "iam" else pytest.fail(f"unexpected client {name}"))
        rc = _mod.main(["--delete-bootstrap-user", "sf-bootstrap"])
        assert rc == 0
        assert "delete_user" in iam.calls


class TestBootstrapArgParsing:
    def test_modes_are_mutually_exclusive(self):
        with pytest.raises(SystemExit):
            _mod.build_arg_parser().parse_args(["--print-bootstrap-policy", "--delete"])

    def test_delete_bootstrap_user_takes_a_name(self):
        args = _mod.build_arg_parser().parse_args(["--delete-bootstrap-user", "sf-bootstrap"])
        assert args.delete_bootstrap_user == "sf-bootstrap"
        assert args.print_bootstrap_policy is False
