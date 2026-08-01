"""
Shared helper for writing resolved AWS setup reference files.

archive-processor and archive-compaction each ship template JSON files
(baked into their own image, under specs/aws/) containing a literal
__BUCKET_NAME__ placeholder. Neither ever calls an AWS provisioning API --
on every startup, each resolves its own templates against its own
already-parsed s3.bucket config and writes the result to
{data_dir}/aws-setup/, for a human to read/copy from when provisioning
Glue/IAM by hand (see docs/aws-setup.md).
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_PLACEHOLDER = "__BUCKET_NAME__"


def write_aws_setup_files(data_dir: str, bucket: str, templates: dict[str, str]) -> None:
    """Resolve each template's __BUCKET_NAME__ against `bucket` and write it
    to {data_dir}/aws-setup/{dest filename}.

    `templates` maps a template file path to the destination filename.
    Always overwrites -- safe to call on every startup, and picks up a
    changed bucket name or an upgraded template on the next restart.
    """
    out_dir = os.path.join(data_dir, "aws-setup")
    os.makedirs(out_dir, exist_ok=True)

    for template_path, dest_filename in templates.items():
        try:
            with open(template_path, "r") as f:
                content = f.read()
        except OSError as exc:
            logger.warning("Could not read AWS setup template %s: %s", template_path, exc)
            continue

        resolved = content.replace(_PLACEHOLDER, bucket)
        dest_path = os.path.join(out_dir, dest_filename)
        try:
            with open(dest_path, "w") as f:
                f.write(resolved)
            logger.info("Wrote resolved AWS setup file: %s", dest_path)
        except OSError as exc:
            logger.warning("Could not write AWS setup file %s: %s", dest_path, exc)
