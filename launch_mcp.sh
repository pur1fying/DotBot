#!/bin/sh
# Repository-relative owner text entry. Dedicated tunnel args supply --db.
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1
exec /usr/bin/env -i PATH=/usr/bin:/bin /usr/bin/python3 -B "$project_dir/service.py" --wechat-owner "$@"
