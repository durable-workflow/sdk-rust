#!/usr/bin/env python3
"""Captured entry point. Invoke these Git-object bytes with Python -I -S -c."""
import argparse
import pathlib
import re
import subprocess
import sys
import types

SUBJECT = 'scripts/acceptance/condition-history/subject.py'


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--checkout', required=True)
    parser.add_argument('--runner-commit', required=True)
    parser.add_argument('--package-commit', required=True)
    args, _ = parser.parse_known_args()
    if args.package_commit != '4b2440a5618ccc1c98960ea2e1e62127042c3590':
        raise SystemExit('wrong fixed package source')
    if not re.fullmatch('[0-9a-f]{40}', args.runner_commit):
        raise SystemExit('invalid runner commit')
    checkout = pathlib.Path(args.checkout).resolve(strict=True)
    result = subprocess.run(['git', '--no-replace-objects', '-C', str(checkout), 'show',
        args.runner_commit + ':' + SUBJECT], check=True, capture_output=True, timeout=20)
    module = types.ModuleType('sealed_subject')
    module.__file__ = 'git:' + args.runner_commit + ':' + SUBJECT
    sys.modules['sealed_subject'] = module
    exec(compile(result.stdout, module.__file__, 'exec'), module.__dict__)
    module.bootstrap()


if __name__ == '__main__':
    main()
