#!/bin/sh
# nextver.sh <patch|minor|major>
# Prints the next semver tag based on the latest git tag. Pre-GA convention:
# first tag is v0.1.0 (PLAN §13.5). Does not tag or push — callers do that.
set -e
bump="${1:-patch}"
cur="$(git describe --tags --abbrev=0 2>/dev/null || echo "")"

if [ -z "$cur" ]; then
	echo "v0.1.0"
	exit 0
fi

v="${cur#v}"
major="${v%%.*}"
rest="${v#*.}"
minor="${rest%%.*}"
patch="${rest#*.}"

case "$bump" in
major) major=$((major + 1)); minor=0; patch=0 ;;
minor) minor=$((minor + 1)); patch=0 ;;
patch) patch=$((patch + 1)) ;;
*)
	echo "usage: nextver.sh <patch|minor|major>" >&2
	exit 2
	;;
esac

echo "v${major}.${minor}.${patch}"
