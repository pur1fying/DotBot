#!/bin/sh
# Opt-in replacement stdio command for the SAME existing tunnel/connection.
# Applying/changing a live launch command and restarting requires user approval.
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1
exec /usr/bin/env -i PATH=/usr/bin:/bin /usr/bin/python3 -B "$project_dir/service.py" --wechat-owner --wechat-media "$@"
