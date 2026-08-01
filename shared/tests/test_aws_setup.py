import os
import tempfile

from shared.aws_setup import write_aws_setup_files


class TestWriteAwsSetupFiles:
    def test_placeholder_resolved(self):
        with tempfile.TemporaryDirectory() as tmp:
            template_path = os.path.join(tmp, "template.json")
            with open(template_path, "w") as f:
                f.write('{"Location": "s3://__BUCKET_NAME__/index/"}')

            data_dir = os.path.join(tmp, "data")
            write_aws_setup_files(data_dir, "my-bucket", {template_path: "resolved.json"})

            dest_path = os.path.join(data_dir, "aws-setup", "resolved.json")
            with open(dest_path) as f:
                content = f.read()
            assert content == '{"Location": "s3://my-bucket/index/"}'

    def test_multiple_templates_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            t1 = os.path.join(tmp, "t1.json")
            t2 = os.path.join(tmp, "t2.json")
            with open(t1, "w") as f:
                f.write("one __BUCKET_NAME__")
            with open(t2, "w") as f:
                f.write("two __BUCKET_NAME__")

            data_dir = os.path.join(tmp, "data")
            write_aws_setup_files(data_dir, "bucket-x", {t1: "one.json", t2: "two.json"})

            out_dir = os.path.join(data_dir, "aws-setup")
            with open(os.path.join(out_dir, "one.json")) as f:
                assert f.read() == "one bucket-x"
            with open(os.path.join(out_dir, "two.json")) as f:
                assert f.read() == "two bucket-x"

    def test_creates_data_dir_if_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            template_path = os.path.join(tmp, "template.json")
            with open(template_path, "w") as f:
                f.write("__BUCKET_NAME__")

            data_dir = os.path.join(tmp, "does", "not", "exist", "yet")
            write_aws_setup_files(data_dir, "b", {template_path: "out.json"})

            assert os.path.exists(os.path.join(data_dir, "aws-setup", "out.json"))

    def test_rerun_overwrites_with_new_bucket(self):
        with tempfile.TemporaryDirectory() as tmp:
            template_path = os.path.join(tmp, "template.json")
            with open(template_path, "w") as f:
                f.write("__BUCKET_NAME__")

            data_dir = os.path.join(tmp, "data")
            write_aws_setup_files(data_dir, "bucket-old", {template_path: "out.json"})
            write_aws_setup_files(data_dir, "bucket-new", {template_path: "out.json"})

            dest_path = os.path.join(data_dir, "aws-setup", "out.json")
            with open(dest_path) as f:
                assert f.read() == "bucket-new"

    def test_missing_template_does_not_raise(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = os.path.join(tmp, "data")
            write_aws_setup_files(
                data_dir, "b", {os.path.join(tmp, "nonexistent.json"): "out.json"}
            )
            assert not os.path.exists(os.path.join(data_dir, "aws-setup", "out.json"))
